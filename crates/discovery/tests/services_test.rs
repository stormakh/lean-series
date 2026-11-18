//! Port of discovery/moby/services_test.go. The mock daemon (mock_test.go) is `moby_mock::SdMock`.
mod common;
mod moby_mock;

use common::{ls, new_options};
use discovery::CancelToken;
use discovery::moby::{Discovery, DockerSwarmSdConfig};
use model::labelset::LabelSet;
use moby_mock::SdMock;

fn new_discovery(cfg: &DockerSwarmSdConfig, set_name: &str) -> Discovery {
    let (mut opts, _reg) = new_options(cfg);
    opts.set_name = set_name.to_string();
    Discovery::new(cfg, opts).unwrap()
}

#[test]
fn test_docker_swarm_sd_services_refresh() {
    let sdmock = SdMock::new("swarmprom");

    let e = sdmock.endpoint();
    let url = &e[..e.len() - 1];
    let cfg_string = format!("\n---\nrole: services\nhost: {url}\n");
    let cfg: DockerSwarmSdConfig = yaml::unmarshal(&cfg_string).unwrap();

    let mut d = new_discovery(&cfg, "moby");

    let ctx = CancelToken::new();
    let tgs = d.refresh(&ctx).unwrap();

    assert_eq!(tgs.len(), 1);

    let tg = &tgs[0];
    assert_eq!(tg.targets.len(), 15);

    let expected: Vec<LabelSet> = vec![
        ls(&[
            ("__address__", "10.0.0.7:9100"),
            ("__meta_dockerswarm_network_id", "qvwhwd6p61k4o0ulsknqb066z"),
            ("__meta_dockerswarm_network_ingress", "true"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_name", "ingress"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "3qvd7bwfptmoht16t1f7jllb6"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-node-exporter:v0.16.0"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "global"),
            ("__meta_dockerswarm_service_name", "mon_node-exporter"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-node-exporter:v0.16.0@sha256:1d2518ec0501dd848e718bf008df37852b1448c862d6f256f2d39244975758d6"),
        ]),
        ls(&[
            ("__address__", "10.0.1.2:9100"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "3qvd7bwfptmoht16t1f7jllb6"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-node-exporter:v0.16.0"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "global"),
            ("__meta_dockerswarm_service_name", "mon_node-exporter"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-node-exporter:v0.16.0@sha256:1d2518ec0501dd848e718bf008df37852b1448c862d6f256f2d39244975758d6"),
        ]),
        ls(&[
            ("__address__", "10.0.1.34:80"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_id", "9bbq7j55tzzz85k2gg52x73rg"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "cloudflare/unsee:v0.8.0"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_unsee"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "cloudflare/unsee:v0.8.0@sha256:28398f47f63feb1647887999701fa730da351fc6d3cc7228e5da44b40a663ada"),
        ]),
        ls(&[
            ("__address__", "10.0.1.13:80"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_id", "hv645udwaaewyw7wuc6nspm68"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-prometheus:v2.5.0"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_prometheus"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-prometheus:v2.5.0@sha256:f1a3781c4785637ba088dcf54889f77d9b6b69f21b2c0a167c1347473f4e2587"),
        ]),
        ls(&[
            ("__address__", "10.0.1.23:80"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_id", "iml8vdd2dudvq457sbirpf66z"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "google/cadvisor"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "global"),
            ("__meta_dockerswarm_service_name", "mon_cadvisor"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "google/cadvisor:latest@sha256:815386ebbe9a3490f38785ab11bda34ec8dacf4634af77b8912832d4f85dca04"),
        ]),
        ls(&[
            ("__address__", "10.0.1.31:80"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_id", "rl4jhdws3r4lkfvq8kx6c4xnr"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-alertmanager:v0.14.0"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_alertmanager"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-alertmanager:v0.14.0@sha256:7069b656bd5df0606ff1db81a7553a25dc72be51d3aca6a7e08d776566cefbd8"),
        ]),
        ls(&[
            ("__address__", "10.0.0.13:9090"),
            ("__meta_dockerswarm_network_id", "qvwhwd6p61k4o0ulsknqb066z"),
            ("__meta_dockerswarm_network_ingress", "true"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_name", "ingress"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "tkv91uso46cck763aaqcb4rek"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/caddy"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_caddy"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/caddy:latest@sha256:44541cfacb66f4799f81f17fcfb3cb757ccc8f327592745549f5930c42d115c9"),
        ]),
        ls(&[
            ("__address__", "10.0.0.13:9093"),
            ("__meta_dockerswarm_network_id", "qvwhwd6p61k4o0ulsknqb066z"),
            ("__meta_dockerswarm_network_ingress", "true"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_name", "ingress"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "tkv91uso46cck763aaqcb4rek"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/caddy"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_caddy"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/caddy:latest@sha256:44541cfacb66f4799f81f17fcfb3cb757ccc8f327592745549f5930c42d115c9"),
        ]),
        ls(&[
            ("__address__", "10.0.0.13:9094"),
            ("__meta_dockerswarm_network_id", "qvwhwd6p61k4o0ulsknqb066z"),
            ("__meta_dockerswarm_network_ingress", "true"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_name", "ingress"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "tkv91uso46cck763aaqcb4rek"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/caddy"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_caddy"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/caddy:latest@sha256:44541cfacb66f4799f81f17fcfb3cb757ccc8f327592745549f5930c42d115c9"),
        ]),
        ls(&[
            ("__address__", "10.0.1.15:9090"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "tkv91uso46cck763aaqcb4rek"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/caddy"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_caddy"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/caddy:latest@sha256:44541cfacb66f4799f81f17fcfb3cb757ccc8f327592745549f5930c42d115c9"),
        ]),
        ls(&[
            ("__address__", "10.0.1.15:9093"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "tkv91uso46cck763aaqcb4rek"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/caddy"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_caddy"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/caddy:latest@sha256:44541cfacb66f4799f81f17fcfb3cb757ccc8f327592745549f5930c42d115c9"),
        ]),
        ls(&[
            ("__address__", "10.0.1.15:9094"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "tkv91uso46cck763aaqcb4rek"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/caddy"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_caddy"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/caddy:latest@sha256:44541cfacb66f4799f81f17fcfb3cb757ccc8f327592745549f5930c42d115c9"),
        ]),
        ls(&[
            ("__address__", "10.0.0.15:3000"),
            ("__meta_dockerswarm_network_id", "qvwhwd6p61k4o0ulsknqb066z"),
            ("__meta_dockerswarm_network_ingress", "true"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_name", "ingress"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "uk9su5tb9ykfzew3qtmp14uzh"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-grafana:5.3.4"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_grafana"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-grafana:5.3.4@sha256:2aca8aa5716e6e0eed3fcdc88fec256a0a1828c491a8cf240486ae7cc473092d"),
        ]),
        ls(&[
            ("__address__", "10.0.1.29:3000"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "uk9su5tb9ykfzew3qtmp14uzh"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-grafana:5.3.4"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_grafana"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-grafana:5.3.4@sha256:2aca8aa5716e6e0eed3fcdc88fec256a0a1828c491a8cf240486ae7cc473092d"),
        ]),
        ls(&[
            ("__address__", "10.0.1.17:80"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_id", "ul5qawv4s7f7msm7dtyfarw80"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/caddy"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "global"),
            ("__meta_dockerswarm_service_name", "mon_dockerd-exporter"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/caddy:latest@sha256:44541cfacb66f4799f81f17fcfb3cb757ccc8f327592745549f5930c42d115c9"),
        ]),
    ];
    for (i, lbls) in expected.iter().enumerate() {
        assert_eq!(lbls, &tg.targets[i], "item {i}");
    }
}

#[test]
fn test_docker_swarm_sd_services_refresh_with_filters() {
    let sdmock = SdMock::new("swarmprom");

    let e = sdmock.endpoint();
    let url = &e[..e.len() - 1];
    let cfg_string = format!("\n---\nrole: services\nhost: {url}\nfilters:\n- name: name\n  values: [\"mon_node-exporter\", \"mon_grafana\"]\n");
    let cfg: DockerSwarmSdConfig = yaml::unmarshal(&cfg_string).unwrap();

    let mut d = new_discovery(&cfg, "moby");

    let ctx = CancelToken::new();
    let tgs = d.refresh(&ctx).unwrap();

    assert_eq!(tgs.len(), 1);

    let tg = &tgs[0];
    assert_eq!(tg.targets.len(), 4, "tg.targets should have 4 targets");

    let expected: Vec<LabelSet> = vec![
        ls(&[
            ("__address__", "10.0.0.7:9100"),
            ("__meta_dockerswarm_network_id", "qvwhwd6p61k4o0ulsknqb066z"),
            ("__meta_dockerswarm_network_ingress", "true"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_name", "ingress"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "3qvd7bwfptmoht16t1f7jllb6"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-node-exporter:v0.16.0"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "global"),
            ("__meta_dockerswarm_service_name", "mon_node-exporter"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-node-exporter:v0.16.0@sha256:1d2518ec0501dd848e718bf008df37852b1448c862d6f256f2d39244975758d6"),
        ]),
        ls(&[
            ("__address__", "10.0.1.2:9100"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "3qvd7bwfptmoht16t1f7jllb6"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-node-exporter:v0.16.0"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "global"),
            ("__meta_dockerswarm_service_name", "mon_node-exporter"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-node-exporter:v0.16.0@sha256:1d2518ec0501dd848e718bf008df37852b1448c862d6f256f2d39244975758d6"),
        ]),
        ls(&[
            ("__address__", "10.0.0.15:3000"),
            ("__meta_dockerswarm_network_id", "qvwhwd6p61k4o0ulsknqb066z"),
            ("__meta_dockerswarm_network_ingress", "true"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_name", "ingress"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "uk9su5tb9ykfzew3qtmp14uzh"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-grafana:5.3.4"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_grafana"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-grafana:5.3.4@sha256:2aca8aa5716e6e0eed3fcdc88fec256a0a1828c491a8cf240486ae7cc473092d"),
        ]),
        ls(&[
            ("__address__", "10.0.1.29:3000"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "uk9su5tb9ykfzew3qtmp14uzh"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-grafana:5.3.4"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_grafana"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-grafana:5.3.4@sha256:2aca8aa5716e6e0eed3fcdc88fec256a0a1828c491a8cf240486ae7cc473092d"),
        ]),
    ];
    for (i, lbls) in expected.iter().enumerate() {
        assert_eq!(lbls, &tg.targets[i], "item {i}");
    }
}

/// Service discovery does not panic when a service has no container spec, which is the case for services whose
/// task runtime is a plugin or a network attachment.
#[test]
fn test_docker_swarm_sd_services_refresh_no_container_spec() {
    let sdmock = SdMock::new("swarmprom-plugin");

    let e = sdmock.endpoint();
    let url = &e[..e.len() - 1];
    let cfg_string = format!("\n---\nrole: services\nhost: {url}\n");
    let cfg: DockerSwarmSdConfig = yaml::unmarshal(&cfg_string).unwrap();

    let mut d = new_discovery(&cfg, "moby");

    let ctx = CancelToken::new();
    let tgs = d.refresh(&ctx).unwrap();

    assert_eq!(tgs.len(), 1);

    let tg = &tgs[0];
    assert_eq!(tg.targets.len(), 2);

    let expected: Vec<LabelSet> = vec![
        ls(&[
            ("__address__", "10.0.1.29:3000"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", ""),
            ("__meta_dockerswarm_service_endpoint_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_service_id", "uk9su5tb9ykfzew3qtmp14uzh"),
            ("__meta_dockerswarm_service_label_com_docker_stack_image", "stefanprodan/swarmprom-grafana:5.3.4"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "mon_grafana"),
            ("__meta_dockerswarm_service_task_container_hostname", ""),
            ("__meta_dockerswarm_service_task_container_image", "stefanprodan/swarmprom-grafana:5.3.4@sha256:2aca8aa5716e6e0eed3fcdc88fec256a0a1828c491a8cf240486ae7cc473092d"),
        ]),
        ls(&[
            ("__address__", "10.0.1.30:80"),
            ("__meta_dockerswarm_network_id", "npq2closzy836m07eaq1425k3"),
            ("__meta_dockerswarm_network_ingress", "false"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_com_docker_stack_namespace", "mon"),
            ("__meta_dockerswarm_network_name", "mon_net"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_id", "tkv91uso46cck763onl5j4q3v"),
            ("__meta_dockerswarm_service_label_com_docker_stack_namespace", "plugins"),
            ("__meta_dockerswarm_service_mode", "global"),
            ("__meta_dockerswarm_service_name", "global_plugin"),
        ]),
    ];
    for (i, lbls) in expected.iter().enumerate() {
        assert_eq!(lbls, &tg.targets[i], "item {i}");
    }
}
