//! Port of `discovery/aws`: the `aws_sd_configs` umbrella config (one role per config) and the per-service
//! configs `ec2`, `ecs`, `elasticache`, `lightsail`, `msk`, `rds` (each also registered as `<name>_sd_configs`).
pub mod client;
pub mod ec2;
pub mod ecs;
pub mod elasticache;
pub mod lightsail;
pub mod msk;
pub mod rds;
pub mod xml;
