//! Port of discovery/moby/nodes_test.go. The mock daemon (mock_test.go) is `moby_mock::SdMock`.
mod common;
mod moby_mock;

use common::{ls, new_options};
use discovery::CancelToken;
use discovery::moby::{Discovery, DockerSwarmSdConfig};
use model::labelset::LabelSet;
use moby_mock::SdMock;

#[test]
fn test_docker_swarm_nodes_sd_refresh() {
    let sdmock = SdMock::new("swarmprom");

    let e = sdmock.endpoint();
    let url = &e[..e.len() - 1];
    let cfg_string = format!("\n---\nrole: nodes\nhost: {url}\n");
    let cfg: DockerSwarmSdConfig = yaml::unmarshal(&cfg_string).unwrap();

    let (mut opts, _reg) = new_options(&cfg);
    opts.set_name = "docker_swarm".to_string();
    let mut d = Discovery::new(&cfg, opts).unwrap();

    let ctx = CancelToken::new();
    let tgs = d.refresh(&ctx).unwrap();

    assert_eq!(tgs.len(), 1);

    let tg = &tgs[0];
    assert_eq!(tg.targets.len(), 5);

    let expected: Vec<LabelSet> = vec![
        ls(&[
            ("__address__", "10.0.232.3:80"),
            ("__meta_dockerswarm_node_address", "10.0.232.3"),
            ("__meta_dockerswarm_node_availability", "active"),
            ("__meta_dockerswarm_node_engine_version", "19.03.11"),
            ("__meta_dockerswarm_node_hostname", "master-3"),
            ("__meta_dockerswarm_node_id", "bvtjl7pnrtg0k88ywialsldpd"),
            ("__meta_dockerswarm_node_manager_address", "10.0.232.3:2377"),
            ("__meta_dockerswarm_node_manager_leader", "false"),
            ("__meta_dockerswarm_node_manager_reachability", "reachable"),
            ("__meta_dockerswarm_node_platform_architecture", "x86_64"),
            ("__meta_dockerswarm_node_platform_os", "linux"),
            ("__meta_dockerswarm_node_role", "manager"),
            ("__meta_dockerswarm_node_status", "ready"),
        ]),
        ls(&[
            ("__address__", "10.0.232.1:80"),
            ("__meta_dockerswarm_node_address", "10.0.232.1"),
            ("__meta_dockerswarm_node_availability", "active"),
            ("__meta_dockerswarm_node_engine_version", "19.03.5-ce"),
            ("__meta_dockerswarm_node_hostname", "oxygen"),
            ("__meta_dockerswarm_node_id", "d3cw2msquo0d71yn42qrnb0tu"),
            ("__meta_dockerswarm_node_manager_address", "10.0.232.1:2377"),
            ("__meta_dockerswarm_node_manager_leader", "true"),
            ("__meta_dockerswarm_node_manager_reachability", "reachable"),
            ("__meta_dockerswarm_node_platform_architecture", "x86_64"),
            ("__meta_dockerswarm_node_platform_os", "linux"),
            ("__meta_dockerswarm_node_role", "manager"),
            ("__meta_dockerswarm_node_status", "ready"),
        ]),
        ls(&[
            ("__address__", "10.0.232.2:80"),
            ("__meta_dockerswarm_node_address", "10.0.232.2"),
            ("__meta_dockerswarm_node_availability", "active"),
            ("__meta_dockerswarm_node_engine_version", "19.03.11"),
            ("__meta_dockerswarm_node_hostname", "master-2"),
            ("__meta_dockerswarm_node_id", "i9woemzxymn1n98o9ufebclgm"),
            ("__meta_dockerswarm_node_manager_address", "10.0.232.2:2377"),
            ("__meta_dockerswarm_node_manager_leader", "false"),
            ("__meta_dockerswarm_node_manager_reachability", "reachable"),
            ("__meta_dockerswarm_node_platform_architecture", "x86_64"),
            ("__meta_dockerswarm_node_platform_os", "linux"),
            ("__meta_dockerswarm_node_role", "manager"),
            ("__meta_dockerswarm_node_status", "ready"),
        ]),
        ls(&[
            ("__address__", "10.0.232.4:80"),
            ("__meta_dockerswarm_node_address", "10.0.232.4"),
            ("__meta_dockerswarm_node_availability", "active"),
            ("__meta_dockerswarm_node_engine_version", "19.03.11"),
            ("__meta_dockerswarm_node_hostname", "worker-1"),
            ("__meta_dockerswarm_node_id", "jkc2xd7p3xdazf0u6sh1m9dmr"),
            ("__meta_dockerswarm_node_platform_architecture", "x86_64"),
            ("__meta_dockerswarm_node_platform_os", "linux"),
            ("__meta_dockerswarm_node_role", "worker"),
            ("__meta_dockerswarm_node_status", "ready"),
        ]),
        ls(&[
            ("__address__", "10.0.232.5:80"),
            ("__meta_dockerswarm_node_address", "10.0.232.5"),
            ("__meta_dockerswarm_node_availability", "active"),
            ("__meta_dockerswarm_node_engine_version", "19.03.11"),
            ("__meta_dockerswarm_node_hostname", "worker-2"),
            ("__meta_dockerswarm_node_id", "ldawcom10uqi6owysgi28n4ve"),
            ("__meta_dockerswarm_node_platform_architecture", "x86_64"),
            ("__meta_dockerswarm_node_platform_os", "linux"),
            ("__meta_dockerswarm_node_role", "worker"),
            ("__meta_dockerswarm_node_status", "ready"),
        ]),
    ];
    for (i, lbls) in expected.iter().enumerate() {
        assert_eq!(lbls, &tg.targets[i], "item {i}");
    }
}
