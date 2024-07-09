use anyhow::Result;
use async_trait::async_trait;
use etcd_client::{Client, PutOptions};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info, warn, trace};

use super::{InstanceInfo, RetryConfig, ServiceInfo, ServiceRegistry};

const DEFAULT_TTL: i64 = 60;

#[derive(Clone)]
pub struct EtcdRegistry {
    lease_ttl: i64,
    client: Client,
    retry_config: RetryConfig,
    stop: broadcast::Sender<bool>,
    drop_key: String,
}

impl Drop for EtcdRegistry{
    fn drop(&mut self) {
        if self.drop_key.is_empty(){
            return;
        }
        let key = self.drop_key.clone();
        let mut client = self.client.clone();
        let _ = self.stop.send(true);
        
        tokio::spawn(async move {
            client.delete(key, None).await.unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(100));  // 阻塞等待etcd删除键值
        info!("etcd delete key");
    }
}

impl EtcdRegistry {
    pub async fn new(endpoints: Vec<String>) -> Self {
        let client = Client::connect(endpoints, None).await.unwrap();
        //let client = Arc::new(client);
        let (shutdown_tx, _) = broadcast::channel(10);
        Self {
            lease_ttl: DEFAULT_TTL,
            client,
            retry_config: RetryConfig::new(),
            stop:shutdown_tx,
            drop_key:"".to_string(),
        }
    }

    async fn self_register(
        &self,
        info: ServiceInfo,
        lease_id: i64,
        tx: mpsc::Sender<(i64, oneshot::Sender<bool>)>,
    ) -> Result<()> {
        let addr = info.addr.to_string();
        let instance = InstanceInfo {
            network: info.protocol.to_string(),
            address: addr.clone(),
            weight: info.weight,
            tags: info.tags,
        };

        let instance_str = serde_json::to_string(&instance)?;

        let key = service_key(&info.service_name, &addr);
        let mut client = self.client.clone();
        let _ = client
            .put(
                key.clone(),
                instance_str.clone(),
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await?;

        let ttl = self.lease_ttl;    
        let client = self.client.clone();
        let retry = self.retry_config.clone();
        let stop = self.stop.subscribe();
        tokio::spawn(async move {
            keep_register(
                client, key, 
                instance_str, 
                ttl,
                retry, 
                tx, 
                stop).await;
        });

        Ok(())
    }

    async fn keepalive(
        &self, 
        mut lease_rx: mpsc::Receiver<(i64, oneshot::Sender<bool>)>
    ) -> Result<()> {
        let client = self.client.clone();
        tokio::spawn(async move { 
            let mut client = client.clone();
            let (sd_tx, _sd_rx) = broadcast::channel(1);

            while let Some((lease, tx)) = lease_rx.recv().await {
                info!("recv: {}",lease);
                let _ = sd_tx.send(true);
                if lease == 0 {
                    break;
                }
                let (mut keeper, mut stream) = match client.lease_keep_alive(lease).await {
                    Ok(v) => (v.0, v.1),
                    Err(e) => {
                        error!("{:?}",e);
                        let _ = tx.send(false);
                        continue;
                    },
                };
            
                let mut sd_tx_clone = sd_tx.subscribe();
                tokio::spawn(async move {
                    let delay = std::time::Duration::from_secs(1);
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {
                                if let Err(e) = keeper.keep_alive().await{
                                    error!("etcd keep alive failed err = {}",e);
                                    break;
                                }
                                if let Some(resp) = stream.message().await.unwrap() {
                                    trace!("lease {:?} keep alive, new ttl {:?}", resp.id(), resp.ttl());
                                }
                            },
                            _= sd_tx_clone.recv() => {
                                info!("keepalive 退出!");
                                break;
                            }
                        }
                    }
                });
                let _ = tx.send(true);
            } 
        });
        Ok(())
    }
}

async fn grant_lease(mut client: Client, ttl: i64) -> Result<i64> {
    let id = client.lease_grant(ttl, None).await?;
    Ok(id.id())
}

async fn keep_register(
    mut client: Client,
    key: String,
    val: String,
    ttl: i64,
    retry_config: RetryConfig,
    tx: mpsc::Sender<(i64, oneshot::Sender<bool>)>,
    mut stop: broadcast::Receiver<bool>,
) {
    let mut failed_times = 0u16;
    let mut delay = retry_config.observe_delay;
    loop {
        if retry_config.max_attempt_times > 0 as u16 
            && failed_times >= retry_config.max_attempt_times
        {
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {
                let resp = match client.get(key.clone(), None).await{
                    Ok(v) => v,
                    Err(e) => {
                        failed_times+=1;
                        delay = retry_config.retry_delay;
                        warn!("keep register get {:?} failed with err: {:?}", key.clone(), e);
                        continue;
                    },
                };
                if resp.kvs().is_empty(){  // 没有值
                    let lease_id = match grant_lease(client.clone(), ttl).await{
                        Ok(v) => v,
                        Err(e) => {
                            warn!("keep register get {} failed with err: {:?}", key, e);
                            delay = retry_config.retry_delay;
                            failed_times += 1;
                            continue;
                        }
                    };
                    if let Err(e) = client
                        .put(
                            key.clone(),
                            val.clone(),
                            Some(PutOptions::new().with_lease(lease_id)),
                    )
                    .await {
                        warn!("keep register put {} failed with err: {:?}", key, e);
                        delay = retry_config.retry_delay;
                        failed_times += 1;
                        continue;
                    };

                    let (one_tx, one_rx) = oneshot::channel::<bool>();
                    if let Err(e) =  tx.send((lease_id, one_tx)).await{
                        warn!("keep register send channel failed with err: {:?}", e);
                        delay = retry_config.retry_delay;
                        failed_times += 1;
                        continue;
                    }

                    delay = retry_config.observe_delay;
                    let _ = one_rx.await;
                }
                failed_times = 0;
            },
            _= stop.recv() => {
                info!("registry 退出");
                break;
            }
        }
    }
}
fn service_key(service_name: &str, addr: &str) -> String {
    format!("kitex/registry-etcd/{}/{}", service_name, addr)
}

#[async_trait]
impl ServiceRegistry for EtcdRegistry {
    async fn register(&mut self, info: ServiceInfo) -> Result<()> {
        info.validate()?;
        let client = self.client.clone();
        let lease_id = grant_lease(client, self.lease_ttl).await?;
        //let rx1 = stop.subscribe();
        let (tx, rx) 
            = mpsc::channel::<(i64, oneshot::Sender<bool>)>(1);
            
        self
            .self_register(info.clone(), lease_id, tx.clone())
            .await
            .unwrap();

        self.keepalive(rx).await?;

        let (one_tx, one_rx) = oneshot::channel();
        let _ = tx.send((lease_id, one_tx)).await?;
        one_rx.await?;

        let addr = info.addr.to_string();
        let key = service_key(&info.service_name, &addr);
        self.drop_key = key;

        Ok(())
    }
    async fn deregister(&self, info: ServiceInfo) -> Result<()> {
        //let key = service_key("test_demo", "192.168.1.250:8001");
        let _ = self.stop.send(true);
        let addr = info.addr.to_string();
        let key = service_key(&info.service_name, &addr);
        let mut client = self.client.clone();
        client.delete(key, None).await?;
        info!("delete");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use tracing::info;

    use super::*;

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
        // let client = EtcdRegistry::new().await;

        // let info = ServiceInfo {
        //     service_name: "test_demo".to_string(),
        //     addr: "192.168.1.250:8081".parse().unwrap(),
        //     payload_codec: "".to_string(),
        //     weight: 10,
        //     start_time: chrono::Local::now(),
        //     warm_up: std::time::Duration::from_secs(10),
        //     tags: HashMap::new(),
        // };

        // let (tx, rx) = broadcast::channel(1);

        // let tx1 = tx.clone();

        // client.register(info, tx1).await?;

        let mut client = Client::connect(["192.168.1.84:2379"], None).await.unwrap();

        client.put("key111", "123123", None).await?;

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        client.delete("key111", None).await?;
        //tokio::time::sleep(std::time::Duration::from_secs(100)).await;

        Ok(())
    }
}
