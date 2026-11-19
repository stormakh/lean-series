//! Port of discovery/stackit/mock_test.go: the STACKIT IaaS API mock.
use crate::common::{Req, Resp, TestServer};

pub const TEST_TOKEN: &str = "LRK9DAWQ1ZAEFSrCNEEzLCUwhYX1U3g7wMg4dTlkkDC96fyDuyJ39nVbVjCKSDfj";
pub const TEST_PROJECT_ID: &str = "00000000-0000-0000-0000-000000000000";

/// Go `SDMock`.
pub struct SdMock {
    pub server: Option<TestServer>,
}

impl SdMock {
    /// Go `NewSDMock`.
    pub fn new() -> SdMock {
        SdMock { server: None }
    }

    /// Go `Endpoint`: the URI of the mock server.
    pub fn endpoint(&self) -> String {
        format!("{}/", self.server.as_ref().expect("Setup").url)
    }

    /// Go `Setup` with `HandleServers` (the mux routes of the mock).
    pub fn setup(&mut self) {
        self.server = Some(TestServer::new(handle_servers));
    }

    /// Go `ShutdownServer`.
    pub fn shutdown_server(&mut self) {
        self.server = None;
    }
}

const SERVERS_BODY: &str = r#"
{
  "items": [
    {
      "availabilityZone": "eu01-3",
      "bootVolume": {
        "deleteOnTermination": false,
        "id": "1c15e4cc-8474-46be-b875-b473ea9fe80c"
      },
      "createdAt": "2025-03-12T14:48:17Z",
      "id": "b4176700-596a-4f80-9fc8-5f9c58a606e1",
      "labels": {
        "provisionSTACKITServerAgent": "true",
        "stackit_project_id": "00000000-0000-0000-0000-000000000000"
      },
      "launchedAt": "2025-03-12T14:48:52Z",
      "machineType": "g1.1",
      "name": "runcommandtest",
      "nics": [
        {
          "ipv4": "10.0.0.153",
          "mac": "fa:16:4f:42:1c:d3",
          "networkId": "3173494f-2f6c-490d-8c12-4b3c86b4338b",
          "networkName": "test",
          "publicIp": "192.0.2.1",
          "nicId": "b36097c5-e1c5-4e12-ae97-c03e144db127",
          "nicSecurity": true,
          "securityGroups": [
            "6e60809f-bed3-46c6-a39c-adddd6455674"
          ]
        }
      ],
      "powerStatus": "STOPPED",
      "serviceAccountMails": [],
      "status": "INACTIVE",
      "updatedAt": "2025-03-13T07:08:29Z",
      "userData": null,
      "volumes": [
        "1c15e4cc-8474-46be-b875-b473ea9fe80c"
      ]
    },
	{
	  "availabilityZone": "eu01-m",
	  "bootVolume": {
	    "deleteOnTermination": false,
	    "id": "1e3ffe2b-878f-46e5-b39e-372e13a09551"
	  },
	  "createdAt": "2025-04-10T16:45:25Z",
	  "id": "ee337436-1f15-4647-a03e-154009966179",
	  "labels": {},
	  "launchedAt": "2025-04-10T16:46:00Z",
	  "machineType": "t1.1",
	  "name": "server1",
	  "nics": [],
	  "powerStatus": "RUNNING",
	  "serviceAccountMails": [],
	  "status": "ACTIVE",
	  "updatedAt": "2025-04-10T16:46:00Z",
	  "volumes": [
	    "1e3ffe2b-878f-46e5-b39e-372e13a09551"
	  ]
  	}
  ]
}"#;

/// Go `HandleServers`: the token endpoint and the servers endpoint (anything else: 404, as Go's ServeMux).
fn handle_servers(r: &Req) -> Resp {
    if r.path == "/token" {
        // Expecting an HTTP form encoded body with the field assertion. A JWT always starts with "ey".
        if !r.body.starts_with(b"assertion=ey") {
            return Resp::new(401, "");
        }
        return Resp::new(200, format!(r#"{{"access_token": "{TEST_TOKEN}"}}"#)).header("content-type", "application/json; charset=utf-8");
    }
    if r.path == format!("/v1/projects/{TEST_PROJECT_ID}/servers") {
        if r.header("Authorization") != format!("Bearer {TEST_TOKEN}") {
            return Resp::new(401, "");
        }
        return Resp::new(200, SERVERS_BODY).header("content-type", "application/json; charset=utf-8");
    }
    Resp::not_found()
}
