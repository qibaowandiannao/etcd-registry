use std::{collections::HashMap, net::SocketAddr};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

mod etcd;

pub use etcd::*;

#[async_trait]
pub trait ServiceRegistry: Send + Sync {
    async fn register(&mut self, info: ServiceInfo) -> Result<()>;
    async fn deregister(&self, info: ServiceInfo) -> Result<()>;
}

#[derive(Clone)]
pub struct RetryConfig {
    max_attempt_times: u16,
    observe_delay: std::time::Duration,
    retry_delay: std::time::Duration,
}

impl RetryConfig {
    pub fn new() -> Self {
        Self {
            max_attempt_times: 5,
            observe_delay: std::time::Duration::from_secs(60),
            retry_delay: std::time::Duration::from_secs(30),
        }
    }
}
/// 服务注册信息
#[derive(Clone)]
pub struct ServiceInfo {
    pub service_name: String,
    pub addr: SocketAddr,
    //pub payload_codec: String,
    pub weight: u32,
    pub protocol: String,
    //pub start_time: chrono::DateTime<Local>,
    //pub warm_up: std::time::Duration,
    pub tags: HashMap<String, String>,
}

impl ServiceInfo {
    pub fn validate(&self) -> Result<()> {
        if self.service_name.is_empty() {
            return Err(anyhow!("missing service name in Register"));
        }
        if self.service_name.contains('/') {
            return Err(anyhow!(
                "service name registered with etcd should not include character '/'"
            ));
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct InstanceInfo {
    network: String,
    address: String,
    weight: u32,
    tags: HashMap<String, String>,
}
