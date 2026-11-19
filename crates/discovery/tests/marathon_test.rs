//! Port of discovery/marathon/marathon_test.go.
mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::{Resp, TestServer, new_options};
use discovery::marathon::{
    App, AppList, AppListClient, Container, DockerContainer, IpAddress, Network, PORT_DEFINITION_LABEL_PREFIX, PORT_MAPPING_LABEL_PREFIX, PortDefinition, PortMapping, SdConfig, Task,
};
use discovery::{CancelToken, Group};

fn marathon_valid_label() -> BTreeMap<String, String> {
    BTreeMap::from([("prometheus".to_string(), "yes".to_string())])
}

fn test_servers() -> Vec<String> {
    vec!["http://localhost:8080".to_string()]
}

fn test_config() -> SdConfig {
    SdConfig { servers: test_servers(), ..Default::default() }
}

fn new_discovery() -> discovery::marathon::Discovery {
    let cfg = test_config();
    let (mut opts, _reg) = new_options(&cfg);
    opts.set_name = "marathon".to_string();
    discovery::marathon::Discovery::new(&cfg, opts).unwrap()
}

fn test_update_services(client: Option<AppListClient>) -> Result<Vec<Arc<Group>>, String> {
    let mut md = new_discovery();
    if let Some(c) = client {
        md.set_apps_client(c);
    }
    md.refresh(&CancelToken::new())
}

fn client(f: fn() -> AppList) -> Option<AppListClient> {
    Some(Box::new(move |_, _, _| Ok(f())))
}

/// `tgt[name]` of a Go `model.LabelSet` ("" when missing).
fn get<'a>(g: &'a Group, i: usize, name: &str) -> &'a str {
    g.targets[i].get(name).map(String::as_str).unwrap_or("")
}

#[test]
fn test_marathon_sd_handle_error() {
    let err_testing = "testing failure";
    let tgs = test_update_services(Some(Box::new(move |_, _, _| Err(err_testing.to_string()))));
    assert_eq!(Err(err_testing.to_string()), tgs);
}

#[test]
fn test_marathon_sd_empty_list() {
    let tgs = test_update_services(client(AppList::default)).unwrap();
    assert!(tgs.is_empty(), "Expected no target groups.");
}

fn marathon_test_app_list(labels: BTreeMap<String, String>, running_tasks: i64) -> AppList {
    let t = Task { id: "test-task-1".into(), host: "mesos-slave1".into(), ..Default::default() };
    let docker = DockerContainer { image: "repo/image:tag".into(), ..Default::default() };
    let port_mappings = vec![PortMapping { labels: labels.clone(), host_port: 31000, ..Default::default() }];
    let container = Container { docker, port_mappings };
    let a = App { id: "test-service".into(), tasks: vec![t], running_tasks, labels, container, ..Default::default() };
    AppList { apps: vec![a] }
}

#[test]
fn test_marathon_sd_send_group() {
    let tgs = test_update_services(client(|| marathon_test_app_list(marathon_valid_label(), 1))).unwrap();
    assert_eq!(1, tgs.len(), "Expected 1 target group.");
    let tg = &tgs[0];
    assert_eq!("test-service", tg.source, "Wrong target group name.");
    assert_eq!(1, tg.targets.len(), "Expected 1 target.");
    assert_eq!("mesos-slave1:31000", get(tg, 0, "__address__"), "Wrong target address.");
    assert_eq!("yes", get(tg, 0, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the first port.");
}

#[test]
fn test_marathon_sd_remove_app() {
    let mut md = new_discovery();
    md.set_apps_client(Box::new(|_, _, _| Ok(marathon_test_app_list(marathon_valid_label(), 1))));
    let tgs = md.refresh(&CancelToken::new()).expect("Got error on first update.");
    assert_eq!(1, tgs.len(), "Expected 1 targetgroup.");
    let tg1 = tgs[0].clone();

    md.set_apps_client(Box::new(|_, _, _| Ok(marathon_test_app_list(marathon_valid_label(), 0))));
    let tgs = md.refresh(&CancelToken::new()).expect("Got error on second update.");
    assert_eq!(1, tgs.len(), "Expected 1 targetgroup.");
    let tg2 = tgs[0].clone();

    assert!(!tg2.targets.is_empty(), "Got a non-empty target set.");
    assert_eq!(tg1.source, tg2.source, "Source is different.");
}

fn marathon_test_app_list_with_multiple_ports(labels: BTreeMap<String, String>, running_tasks: i64) -> AppList {
    let t = Task { id: "test-task-1".into(), host: "mesos-slave1".into(), ..Default::default() };
    let docker = DockerContainer { image: "repo/image:tag".into(), ..Default::default() };
    let port_mappings = vec![
        PortMapping { labels: labels.clone(), host_port: 31000, ..Default::default() },
        PortMapping { labels: BTreeMap::new(), host_port: 32000, ..Default::default() },
    ];
    let container = Container { docker, port_mappings };
    let a = App { id: "test-service".into(), tasks: vec![t], running_tasks, labels, container, ..Default::default() };
    AppList { apps: vec![a] }
}

#[test]
fn test_marathon_sd_send_group_with_multiple_port() {
    let tgs = test_update_services(client(|| marathon_test_app_list_with_multiple_ports(marathon_valid_label(), 1))).unwrap();
    assert_eq!(1, tgs.len(), "Expected 1 target group.");
    let tg = &tgs[0];
    assert_eq!("test-service", tg.source, "Wrong target group name.");
    assert_eq!(2, tg.targets.len(), "Wrong number of targets.");

    assert_eq!("mesos-slave1:31000", get(tg, 0, "__address__"), "Wrong target address.");
    assert_eq!("yes", get(tg, 0, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the first port: {}", get(tg, 0, "__address__"));

    assert_eq!("mesos-slave1:32000", get(tg, 1, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 1, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the second port: {}", get(tg, 1, "__address__"));
}

fn marathon_test_zero_task_port_app_list(labels: BTreeMap<String, String>, running_tasks: i64) -> AppList {
    let t = Task { id: "test-task-2".into(), host: "mesos-slave-2".into(), ports: vec![], ..Default::default() };
    let docker = DockerContainer { image: "repo/image:tag".into(), ..Default::default() };
    let container = Container { docker, ..Default::default() };
    let a = App { id: "test-service-zero-ports".into(), tasks: vec![t], running_tasks, labels, container, ..Default::default() };
    AppList { apps: vec![a] }
}

#[test]
fn test_marathon_zero_task_ports() {
    let tgs = test_update_services(client(|| marathon_test_zero_task_port_app_list(marathon_valid_label(), 1))).unwrap();
    assert_eq!(1, tgs.len(), "Expected 1 target group.");
    let tg = &tgs[0];
    assert_eq!("test-service-zero-ports", tg.source, "Wrong target group name.");
    assert!(tg.targets.is_empty(), "Wrong number of targets.");
}

#[test]
fn test500_error_http_response_with_valid_json_body() {
    // Simulate 500 error with a valid JSON response.
    let _ts = TestServer::new(|_| Resp::new(500, "{}").header("Content-Type", "application/json"));
    // Execute test case and validate behavior (as in Go, the request goes to the configured server, not to
    // the test server).
    let r = test_update_services(None);
    assert!(r.is_err(), "Expected error for 5xx HTTP response from marathon server.");
}

fn marathon_test_app_list_with_port_definitions(labels: BTreeMap<String, String>, running_tasks: i64) -> AppList {
    // Auto-generated ports when requirePorts is false.
    let t = Task { id: "test-task-1".into(), host: "mesos-slave1".into(), ports: vec![1234, 5678], ..Default::default() };
    let docker = DockerContainer { image: "repo/image:tag".into(), ..Default::default() };
    let container = Container { docker, ..Default::default() };
    let a = App {
        id: "test-service".into(),
        tasks: vec![t],
        running_tasks,
        labels: labels.clone(),
        container,
        port_definitions: vec![PortDefinition { labels: BTreeMap::new(), port: 31000 }, PortDefinition { labels, port: 32000 }],
        require_ports: false, // default
        ..Default::default()
    };
    AppList { apps: vec![a] }
}

#[test]
fn test_marathon_sd_send_group_with_port_definitions() {
    let tgs = test_update_services(client(|| marathon_test_app_list_with_port_definitions(marathon_valid_label(), 1))).unwrap();
    assert_eq!(1, tgs.len(), "Expected 1 target group.");
    let tg = &tgs[0];
    assert_eq!("test-service", tg.source, "Wrong target group name.");
    assert_eq!(2, tg.targets.len(), "Wrong number of targets.");

    assert_eq!("mesos-slave1:1234", get(tg, 0, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 0, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the first port.");
    assert_eq!("", get(tg, 0, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the first port.");

    assert_eq!("mesos-slave1:5678", get(tg, 1, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 1, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the second port.");
    assert_eq!("yes", get(tg, 1, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the second port.");
}

fn marathon_test_app_list_with_port_definitions_require_ports(labels: BTreeMap<String, String>, running_tasks: i64) -> AppList {
    let t = Task { id: "test-task-1".into(), host: "mesos-slave1".into(), ports: vec![31000, 32000], ..Default::default() };
    let docker = DockerContainer { image: "repo/image:tag".into(), ..Default::default() };
    let container = Container { docker, ..Default::default() };
    let a = App {
        id: "test-service".into(),
        tasks: vec![t],
        running_tasks,
        labels: labels.clone(),
        container,
        port_definitions: vec![PortDefinition { labels: BTreeMap::new(), port: 31000 }, PortDefinition { labels, port: 32000 }],
        require_ports: true,
        ..Default::default()
    };
    AppList { apps: vec![a] }
}

#[test]
fn test_marathon_sd_send_group_with_port_definitions_require_ports() {
    let tgs = test_update_services(client(|| marathon_test_app_list_with_port_definitions_require_ports(marathon_valid_label(), 1))).unwrap();
    assert_eq!(1, tgs.len(), "Expected 1 target group.");
    let tg = &tgs[0];
    assert_eq!("test-service", tg.source, "Wrong target group name.");
    assert_eq!(2, tg.targets.len(), "Wrong number of targets.");

    assert_eq!("mesos-slave1:31000", get(tg, 0, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 0, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the first port.");
    assert_eq!("", get(tg, 0, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the first port.");

    assert_eq!("mesos-slave1:32000", get(tg, 1, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 1, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the second port.");
    assert_eq!("yes", get(tg, 1, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the second port.");
}

fn marathon_test_app_list_with_ports(labels: BTreeMap<String, String>, running_tasks: i64) -> AppList {
    let t = Task { id: "test-task-1".into(), host: "mesos-slave1".into(), ports: vec![31000, 32000], ..Default::default() };
    let docker = DockerContainer { image: "repo/image:tag".into(), ..Default::default() };
    let container = Container { docker, ..Default::default() };
    let a = App { id: "test-service".into(), tasks: vec![t], running_tasks, labels, container, ..Default::default() };
    AppList { apps: vec![a] }
}

#[test]
fn test_marathon_sd_send_group_with_ports() {
    let tgs = test_update_services(client(|| marathon_test_app_list_with_ports(marathon_valid_label(), 1))).unwrap();
    assert_eq!(1, tgs.len(), "Expected 1 target group.");
    let tg = &tgs[0];
    assert_eq!("test-service", tg.source, "Wrong target group name.");
    assert_eq!(2, tg.targets.len(), "Wrong number of targets.");

    assert_eq!("mesos-slave1:31000", get(tg, 0, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 0, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the first port.");
    assert_eq!("", get(tg, 0, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the first port.");

    assert_eq!("mesos-slave1:32000", get(tg, 1, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 1, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the second port.");
    assert_eq!("", get(tg, 1, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the second port.");
}

fn marathon_test_app_list_with_container_port_mappings(labels: BTreeMap<String, String>, running_tasks: i64) -> AppList {
    let t = Task {
        id: "test-task-1".into(),
        host: "mesos-slave1".into(),
        ports: vec![
            12345, // 'Automatically-generated' port
            32000,
        ],
        ..Default::default()
    };
    let docker = DockerContainer { image: "repo/image:tag".into(), ..Default::default() };
    let container = Container {
        docker,
        port_mappings: vec![
            PortMapping { labels: labels.clone(), host_port: 0, ..Default::default() },
            PortMapping { labels: BTreeMap::new(), host_port: 32000, ..Default::default() },
        ],
    };
    let a = App { id: "test-service".into(), tasks: vec![t], running_tasks, labels, container, ..Default::default() };
    AppList { apps: vec![a] }
}

#[test]
fn test_marathon_sd_send_group_with_container_port_mappings() {
    let tgs = test_update_services(client(|| marathon_test_app_list_with_container_port_mappings(marathon_valid_label(), 1))).unwrap();
    assert_eq!(1, tgs.len(), "Expected 1 target group.");
    let tg = &tgs[0];
    assert_eq!("test-service", tg.source, "Wrong target group name.");
    assert_eq!(2, tg.targets.len(), "Wrong number of targets.");

    assert_eq!("mesos-slave1:12345", get(tg, 0, "__address__"), "Wrong target address.");
    assert_eq!("yes", get(tg, 0, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the first port.");
    assert_eq!("", get(tg, 0, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the first port.");

    assert_eq!("mesos-slave1:32000", get(tg, 1, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 1, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the second port.");
    assert_eq!("", get(tg, 1, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the second port.");
}

fn marathon_test_app_list_with_docker_container_port_mappings(labels: BTreeMap<String, String>, running_tasks: i64) -> AppList {
    let t = Task {
        id: "test-task-1".into(),
        host: "mesos-slave1".into(),
        ports: vec![
            31000, 12345, // 'Automatically-generated' port
        ],
        ..Default::default()
    };
    let docker = DockerContainer {
        image: "repo/image:tag".into(),
        port_mappings: vec![
            PortMapping { labels: labels.clone(), host_port: 31000, ..Default::default() },
            PortMapping { labels: BTreeMap::new(), host_port: 0, ..Default::default() },
        ],
    };
    let container = Container { docker, ..Default::default() };
    let a = App { id: "test-service".into(), tasks: vec![t], running_tasks, labels, container, ..Default::default() };
    AppList { apps: vec![a] }
}

#[test]
fn test_marathon_sd_send_group_with_docker_container_port_mappings() {
    let tgs = test_update_services(client(|| marathon_test_app_list_with_docker_container_port_mappings(marathon_valid_label(), 1))).unwrap();
    assert_eq!(1, tgs.len(), "Expected 1 target group.");
    let tg = &tgs[0];
    assert_eq!("test-service", tg.source, "Wrong target group name.");
    assert_eq!(2, tg.targets.len(), "Wrong number of targets.");

    assert_eq!("mesos-slave1:31000", get(tg, 0, "__address__"), "Wrong target address.");
    assert_eq!("yes", get(tg, 0, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the first port.");
    assert_eq!("", get(tg, 0, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the first port.");

    assert_eq!("mesos-slave1:12345", get(tg, 1, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 1, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the second port.");
    assert_eq!("", get(tg, 1, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the second port.");
}

fn marathon_test_app_list_with_container_network_and_port_mappings(labels: BTreeMap<String, String>, running_tasks: i64) -> AppList {
    let t = Task { id: "test-task-1".into(), host: "mesos-slave1".into(), ip_addresses: vec![IpAddress { address: "1.2.3.4".into(), ..Default::default() }], ..Default::default() };
    let docker = DockerContainer { image: "repo/image:tag".into(), ..Default::default() };
    let port_mappings = vec![
        PortMapping { labels: labels.clone(), container_port: 8080, host_port: 31000, ..Default::default() },
        PortMapping { labels: BTreeMap::new(), container_port: 1234, host_port: 32000, ..Default::default() },
    ];
    let container = Container { docker, port_mappings };
    let networks = vec![Network { mode: "container".into(), name: "test-network".into() }];
    let a = App { id: "test-service".into(), tasks: vec![t], running_tasks, labels, container, networks, ..Default::default() };
    AppList { apps: vec![a] }
}

#[test]
fn test_marathon_sd_send_group_with_container_network_and_port_mapping() {
    let tgs = test_update_services(client(|| marathon_test_app_list_with_container_network_and_port_mappings(marathon_valid_label(), 1))).unwrap();
    assert_eq!(1, tgs.len(), "Expected 1 target group.");
    let tg = &tgs[0];
    assert_eq!("test-service", tg.source, "Wrong target group name.");
    assert_eq!(2, tg.targets.len(), "Wrong number of targets.");

    assert_eq!("1.2.3.4:8080", get(tg, 0, "__address__"), "Wrong target address.");
    assert_eq!("yes", get(tg, 0, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the first port.");
    assert_eq!("", get(tg, 0, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the first port.");

    assert_eq!("1.2.3.4:1234", get(tg, 1, "__address__"), "Wrong target address.");
    assert_eq!("", get(tg, 1, &format!("{PORT_MAPPING_LABEL_PREFIX}prometheus")), "Wrong portMappings label from the second port.");
    assert_eq!("", get(tg, 1, &format!("{PORT_DEFINITION_LABEL_PREFIX}prometheus")), "Wrong portDefinitions label from the second port.");
}

/// Extra (not in the ledger): the JSON decoding of an app list as Go's encoding/json does it.
#[test]
fn extra_fetch_apps_json() {
    let body = r#"{"apps":[{"id":"/a","tasksRunning":1,"Labels":{"x":"y"},"container":{"docker":{"image":"img"},"portMappings":[{"hostPort":0,"labels":{"l":"v"}}]},"tasks":[{"id":"t1","host":"h","ports":[31000]}]}]}"#;
    let ts = TestServer::new(move |_| Resp::json(body));
    let c = net::Client::new(net::new_round_tripper_from_config(&Default::default(), &Default::default()).unwrap());
    let apps = discovery::marathon::fetch_apps(&CancelToken::new(), &c, &format!("{}{}", ts.url, discovery::marathon::APP_LIST_PATH)).unwrap();
    assert_eq!(1, apps.apps.len());
    assert_eq!("y", apps.apps[0].labels["x"]);
    let groups = discovery::marathon::apps_to_target_groups(&apps);
    let g = &groups["/a"];
    assert_eq!("h:31000", get(g, 0, "__address__"));
    assert_eq!("v", get(g, 0, &format!("{PORT_MAPPING_LABEL_PREFIX}l")));
    assert_eq!("/v2/apps/?embed=apps.tasks", ts.requests()[0].uri);

    let bad = r#"{"apps":[{"id":"/a","tasks":[{"ports":["x"]}]}]}"#;
    let ts = TestServer::new(move |_| Resp::json(bad));
    let url = format!("{}/x", ts.url);
    let err = discovery::marathon::fetch_apps(&CancelToken::new(), &c, &url).unwrap_err();
    assert_eq!(format!("\"{url}\": json: cannot unmarshal string into Go struct field task.apps.tasks.ports of type uint32"), err);
}
