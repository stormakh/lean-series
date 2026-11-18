//! Port of discovery/stackit/server_test.go.
use crate::common::ls;
use crate::stackit_mock::{SdMock, TEST_PROJECT_ID, TEST_TOKEN};
use discovery::CancelToken;
use discovery::stackit::{SdConfig, default_sd_config, new_server_discovery};

/// adapted: Go generates a new 2048-bit RSA key with `rsa.GenerateKey` (PKCS#1 PEM); `ring` cannot generate RSA
/// keys, so this is a fixed 2048-bit key made with `openssl genrsa -traditional 2048`. The mock only checks that
/// the token request carries a JWT, so any valid key is equivalent.
const TEST_RSA_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA4LW8rYg6SJaT2POQkrIgHJRG1IQizroLNHBud/3fRMkYRZPo
xwoZ6Z2rWNwJPSCNZOHzLq0n7Nr2jHrHN47K1k0KfI0yMZXHuIJfx41ovOcfpJxp
0CEv5zN/VUu5TjbNNv4ITx+WLXa1K3vb01k5/FuYbLiUC5j7YoE7KVd+39tbTkgS
3f6iLfafbzkRhZpCoTLeeTfuezUBE19VuLTN3dju+uBylyuJ8BNebBaDKBVTHska
2FyE6xLIlwd35uMgRiecwJJtxBq0L0vxdrleatNNswbz+Wr33IEhuJxTgjH+FRyW
wUq3pI5P+PWORJ6nk3tlva3i5X7l+3s/pUpcswIDAQABAoIBAAbtzlkYKRqW5g0x
CXq0hQ/3NINAuyVeZZJi4HfRELg5bocAC2lOcu5vXwVjl4yr0HWqup8XAMhUF3ki
dLp5ATaAHqbTixeDU6q+qRRpVFr1fNYhv+jb38hvH+nckWS2L41sDOE1C0zwlZ/h
zFf6hDK3YlOr514U2V+NYOp37mDEVIMhMAAfIi7z8Sl6fU2ra6xvbdLSwd8p35w1
NWNsdK19KWY2u1XGdyv+8Jj7nL8Lo0B/qAetnLyt/m0q3fkrRYAMBT6iJwuOgM7F
osrg6RNvBiFlUi8AU5sDcV9Pl7HeBSf1Cya21aV0o8PEJs51v9K5nKpwWrdGI0od
ysAESH0CgYEA9eczchVF+7vGk1nMkqkHCk5cOPHX/tAUzGe6DuNtmzvFLQq1Z8Mf
+5+v/NeKTcRxkA1BNv7oNrkrVZxVcBPAPalyr8b5uQ2mP0qW86aF+tsjQf3wxzaf
yOdvpuG49gUMinanVj9AaqgcbD1Xo4tY59Z2HDSq1vIK7Rc9kOkKNl0CgYEA6e/D
ueQWoSggiRBhL28YRoJJ1GPEbrJblYyx+YqT2QODx0DeOb3LtzcvjpqUpWA2gTLR
MTALIRSyNXSb+GhK5RAsXpZ7a4yk7dacQm4VCJp2xy71ljlH4wL0a9EWHv+Owyfx
vnWALGR6Au3mAovvVpNkq5m7LZxQvz2qwA0sjk8CgYEAphWKAbuskOG6tTspmMTh
JGzYjP2LTWdtJMHKQNQK65XrDwj0jSiVKXYpfzR1DSFpMkpBicuRO6TFSAVXD2PH
q94Ov/o8IGi/qG/QlLAcPdpUbfX98g9KzH7EQoqiSkgOuFZ7nx0JfQAeclzcHHG5
kpyfGV3ZJWRFTO0tf/pmE2ECgYBqz6l7H7RhYrPwQQVOphfcYbKaAe93Z4I/dxzv
xiEZTeyGNo0AqjLwdYcGQngqRhK8zNoM7lW2hf+UPqjUM7I2gjrv+R9jDLVlTr7L
6f06j2Q6L6u8Km0zLm8xJydFggPKAVv0TSv3mOzIl5yUfxYJXoTxgOCgw6VunBZx
V9KHKwKBgQDc4pUDUySvCDS1wImbUQhpWLpVmW/GcCieqT4mWx/8ulC8i55ZNFec
22hQeOBKCOXRlC2VBEHTy65lptiItKX6R9uBT5dgHd44zi72sg3JkS6ljePpoZOd
Baauj1lnrI/UycJKDp/lfzHakGuHaUxiE4G75vqdS/ACLP32qerQBg==
-----END RSA PRIVATE KEY-----
";

const SERVICE_ACCOUNT_KEY: &str = r#"{
  "Active": true,
  "CreatedAt": "2025-04-05T12:34:56Z",
  "Credentials": {
    "Aud": "https://stackit-service-account-prod.apps.01.cf.eu01.stackit.cloud",
    "Iss": "stackit@sa.stackit.cloud",
    "Kid": "123e4567-e89b-12d3-a456-426614174000",
    "Sub": "123e4567-e89b-12d3-a456-426614174001"
  },
  "ID": "123e4567-e89b-12d3-a456-426614174002",
  "KeyAlgorithm": "RSA_2048",
  "KeyOrigin": "USER_PROVIDED",
  "KeyType": "USER_MANAGED",
  "PublicKey": "...",
  "ValidUntil": "2025-04-05T13:34:56Z"
}"#;

/// Go `serverSDTestSuite`.
struct ServerSdTestSuite {
    mock: SdMock,
}

impl ServerSdTestSuite {
    /// Go `SetupTest`.
    fn setup_test() -> ServerSdTestSuite {
        let mut mock = SdMock::new();
        mock.setup();
        ServerSdTestSuite { mock }
    }
}

#[test]
fn test_server_sd_refresh() {
    let cases: Vec<(&str, SdConfig)> = vec![
        ("default with token", {
            let mut cfg = default_sd_config();
            cfg.http_client_config.bearer_token = yaml::Secret(TEST_TOKEN.to_string());
            cfg
        }),
        ("default with service account key", {
            let mut cfg = default_sd_config();
            cfg.private_key = yaml::Secret(TEST_RSA_PRIVATE_KEY.to_string());
            cfg.service_account_key = yaml::Secret(SERVICE_ACCOUNT_KEY.to_string());
            cfg
        }),
    ];
    for (name, mut cfg) in cases {
        let mut suite = ServerSdTestSuite::setup_test();

        cfg.endpoint = suite.mock.endpoint();
        cfg.token_url = format!("{}token", suite.mock.endpoint());
        cfg.project = TEST_PROJECT_ID.to_string();

        let mut d = new_server_discovery(&cfg, discovery::Logger::nop()).unwrap_or_else(|e| panic!("{name}: {e}"));
        let target_groups = d.refresh(&CancelToken::new()).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(target_groups.len(), 1, "{name}");

        let target_group = &target_groups[0];
        assert_eq!(target_group.targets.len(), 1, "{name}");

        for (i, label_set) in [ls(&[
            ("__address__", "192.0.2.1:80"),
            ("__meta_stackit_project", "00000000-0000-0000-0000-000000000000"),
            ("__meta_stackit_id", "b4176700-596a-4f80-9fc8-5f9c58a606e1"),
            ("__meta_stackit_type", "g1.1"),
            ("__meta_stackit_private_ipv4_test", "10.0.0.153"),
            ("__meta_stackit_public_ipv4", "192.0.2.1"),
            ("__meta_stackit_labelpresent_provisionSTACKITServerAgent", "true"),
            ("__meta_stackit_label_provisionSTACKITServerAgent", "true"),
            ("__meta_stackit_labelpresent_stackit_project_id", "true"),
            ("__meta_stackit_name", "runcommandtest"),
            ("__meta_stackit_availability_zone", "eu01-3"),
            ("__meta_stackit_status", "INACTIVE"),
            ("__meta_stackit_power_status", "STOPPED"),
            ("__meta_stackit_label_stackit_project_id", "00000000-0000-0000-0000-000000000000"),
        ])]
        .iter()
        .enumerate()
        {
            assert_eq!(&target_group.targets[i], label_set, "{name}");
        }
        suite.mock.shutdown_server();
    }
}
