//! Port of `discovery/aws`: the `aws_sd_configs` umbrella config (one role per config) and the per-service
//! configs `ec2`, `ecs`, `elasticache`, `lightsail`, `msk`, `rds` (each also registered as `<name>_sd_configs`).
pub mod client;
pub mod credentials;
pub mod ec2;
pub mod ecs;
pub mod elasticache;
pub mod labels;
pub mod lightsail;
pub mod msk;
pub mod rds;
pub mod xml;

/// Go `Filter`: a filter for AWS resources (ec2, rds).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Filter {
    pub name: String,
    pub values: Vec<String>,
}

yaml::yaml_struct!(Filter, "aws.Filter", [
    (name: "name")
    (values: "values")
]);
yaml::impl_plain_struct!(Filter);
