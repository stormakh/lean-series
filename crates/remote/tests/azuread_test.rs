//! Port of `storage/remote/azuread/azuread_test.go`. The testify mock of `azcore.TokenCredential` is a small
//! Rust mock with the same rules: expectations are matched in order, `once` ones are used up, and an unmatched
//! call fails the test.
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use config::azuread::{AzureAdConfig, ManagedIdentityConfig, OAuthConfig, SdkConfig, WorkloadIdentityConfig};
use net::{Client, Header, Request, Response};
use net::roundtrip::FnRoundTripper;
use remote::azuread::{
    AZURE_FEDERATED_TOKEN_FILE_ENV_VAR, AccessToken, AzureAdRoundTripper, DEFAULT_WORKLOAD_IDENTITY_TOKEN_PATH, INGESTION_CHINA_AUDIENCE,
    INGESTION_PUBLIC_AUDIENCE, TokenCredential, TokenProvider, TokenRequestOptions, new_token_provider,
};
use yaml::Secret;

const DUMMY_CLIENT_ID: &str = "00000000-0000-0000-0000-000000000000";
const DUMMY_CLIENT_SECRET: &str = "Cl1ent$ecret!";
const DUMMY_TENANT_ID: &str = "00000000-a12b-3cd4-e56f-000000000000";
const TEST_TOKEN_STRING: &str = "testTokenString";

fn test_token_expiry() -> SystemTime {
    SystemTime::now() + Duration::from_secs(5)
}

type Matcher = Box<dyn Fn(&TokenRequestOptions) -> bool + Send + Sync>;

struct Expectation {
    matcher: Option<Matcher>,
    ret: AccessToken,
    once: bool,
    used: bool,
}

/// mockCredential mocks azidentity TokenCredential interface.
#[derive(Default)]
struct MockCredential {
    expected_calls: Mutex<Vec<Expectation>>,
    calls: Mutex<usize>,
}

impl MockCredential {
    /// `On("GetToken", mock.Anything, matcher).Return(ret, nil)`, with `.Once()` when `once`.
    fn on(&self, matcher: Option<Matcher>, ret: AccessToken, once: bool) {
        self.expected_calls.lock().unwrap().push(Expectation { matcher, ret, once, used: false });
    }
    fn reset(&self) {
        self.expected_calls.lock().unwrap().clear();
    }
    fn assert_number_of_calls(&self, n: usize) {
        assert_eq!(n, *self.calls.lock().unwrap(), "Expected number of calls ({n}) does not match the actual number of calls");
    }
}

impl TokenCredential for MockCredential {
    fn get_token(&self, options: &TokenRequestOptions) -> Result<AccessToken, String> {
        let mut ex = self.expected_calls.lock().unwrap();
        *self.calls.lock().unwrap() += 1;
        for e in ex.iter_mut() {
            if (e.once && e.used) || !e.matcher.as_ref().is_none_or(|m| m(options)) {
                continue;
            }
            e.used = true;
            return Ok(e.ret.clone());
        }
        panic!("mock: I don't know what to return because the method call was unexpected: GetToken({options:?})");
    }
}

fn managed_identity_cfg(cloud: &str) -> AzureAdConfig {
    AzureAdConfig { cloud: cloud.into(), managed_identity: Some(ManagedIdentityConfig { client_id: DUMMY_CLIENT_ID.into() }), ..Default::default() }
}

fn oauth_cfg(cloud: &str) -> AzureAdConfig {
    AzureAdConfig {
        cloud: cloud.into(),
        oauth: Some(OAuthConfig { client_id: DUMMY_CLIENT_ID.into(), client_secret: Secret::new(DUMMY_CLIENT_SECRET), tenant_id: DUMMY_TENANT_ID.into() }),
        ..Default::default()
    }
}

fn sdk_cfg(cloud: &str) -> AzureAdConfig {
    AzureAdConfig { cloud: cloud.into(), sdk: Some(SdkConfig { tenant_id: DUMMY_TENANT_ID.into() }), ..Default::default() }
}

fn workload_identity_cfg(cloud: &str) -> AzureAdConfig {
    AzureAdConfig {
        cloud: cloud.into(),
        workload_identity: Some(WorkloadIdentityConfig {
            client_id: DUMMY_CLIENT_ID.into(),
            tenant_id: DUMMY_TENANT_ID.into(),
            token_file_path: DEFAULT_WORKLOAD_IDENTITY_TOKEN_PATH.into(),
        }),
        ..Default::default()
    }
}

/// The suite `AzureAdTestSuite` with its one test `TestAzureAdRoundTripper` (BeforeTest makes a new mock).
#[test]
fn test_azure_ad() {
    // TestAzureAdRoundTripper
    let mock_credential = Arc::new(MockCredential::default());
    let cases = [
        // AzureAd roundtripper with ManagedIdentity.
        managed_identity_cfg("AzurePublic"),
        // AzureAd roundtripper with OAuth.
        oauth_cfg("AzurePublic"),
        // AzureAd roundtripper with Workload Identity.
        workload_identity_cfg("AzurePublic"),
    ];
    for c in &cases {
        let got_req: Arc<Mutex<Option<Request>>> = Arc::new(Mutex::new(None));

        let test_token = AccessToken { token: TEST_TOKEN_STRING.into(), expires_on: test_token_expiry() };
        mock_credential.on(None, test_token, false);

        let token_provider = new_token_provider(c, mock_credential.clone()).expect("newTokenProvider");

        let g = got_req.clone();
        let rt = AzureAdRoundTripper {
            next: Arc::new(FnRoundTripper(move |req: Request| {
                *g.lock().unwrap() = Some(req.clone());
                Ok(Response { status: 200, status_text: "200 OK".into(), header: Header::new(), body: Vec::new(), request: req })
            })),
            token_provider,
        };

        let cli = Client::new(Arc::new(rt));

        let req = Request::new("POST", "https://example.com", b"Hello, world!".to_vec()).expect("NewRequest");

        cli.do_request(req).expect("Do");
        let got = got_req.lock().unwrap().clone();
        assert!(got.is_some());

        let orig_req = got.unwrap();
        assert!(!orig_req.header.get_str("Authorization").is_empty());
        assert_eq!(format!("Bearer {TEST_TOKEN_STRING}"), orig_req.header.get_str("Authorization"));
    }
}

fn load_azure_ad_config(filename: &str) -> Result<AzureAdConfig, String> {
    let path = format!("{}/../../upstream/storage/remote/azuread/{filename}", env!("CARGO_MANIFEST_DIR"));
    let content = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    yaml::unmarshal_strict::<AzureAdConfig>(&content).map_err(|e| e.to_string())
}

#[test]
fn test_azure_ad_config() {
    let cases: &[(&str, &str)] = &[
        // Missing managedidentity or oauth field.
        ("testdata/azuread_bad_configmissing.yaml", "must provide an Azure Managed Identity, Azure Workload Identity, Azure OAuth, Azure Certificate or Azure SDK in the Azure AD config"),
        // Invalid managedidentity client id.
        ("testdata/azuread_bad_invalidclientid.yaml", "the provided Azure Managed Identity client_id is invalid"),
        // Missing tenant id in oauth config.
        ("testdata/azuread_bad_invalidoauthconfig.yaml", "must provide an Azure OAuth tenant_id in the Azure AD config"),
        // Invalid config when both managedidentity and oauth is provided.
        ("testdata/azuread_bad_twoconfig.yaml", "cannot provide multiple authentication methods in the Azure AD config"),
        // Invalid config when both sdk and oauth is provided.
        ("testdata/azuread_bad_oauthsdkconfig.yaml", "cannot provide multiple authentication methods in the Azure AD config"),
        // Invalid workload identity client id.
        ("testdata/azuread_bad_workloadidentity_invalidclientid.yaml", "the provided Azure Workload Identity client_id is invalid"),
        // Invalid workload identity tenant id.
        ("testdata/azuread_bad_workloadidentity_invalidtenantid.yaml", "the provided Azure Workload Identity tenant_id is invalid"),
        // Missing workload identity client id.
        ("testdata/azuread_bad_workloadidentity_missingclientid.yaml", "must provide an Azure Workload Identity client_id in the Azure AD config"),
        // Missing workload identity tenant id.
        ("testdata/azuread_bad_workloadidentity_missingtenantid.yaml", "must provide an Azure Workload Identity tenant_id in the Azure AD config"),
        // Invalid scope validation.
        ("testdata/azuread_bad_scope_invalid.yaml", "the provided scope contains invalid characters"),
        // Valid config with missing  optionally cloud field.
        ("testdata/azuread_good_cloudmissing.yaml", ""),
        // Valid specific managed identity config.
        ("testdata/azuread_good_specificmanagedidentity.yaml", ""),
        // Valid default managed identity config.
        ("testdata/azuread_good_defaultmanagedidentity.yaml", ""),
        // Valid Oauth config.
        ("testdata/azuread_good_oauth.yaml", ""),
        // Valid SDK config.
        ("testdata/azuread_good_sdk.yaml", ""),
        // Valid workload identity config.
        ("testdata/azuread_good_workloadidentity.yaml", ""),
        // Valid OAuth config with custom scope.
        ("testdata/azuread_good_oauth_customscope.yaml", ""),
        // Valid certificate config.
        ("testdata/azuread_good_certificate.yaml", ""),
        // Valid certificate config with separate key file.
        ("testdata/azuread_good_certificate_with_key.yaml", ""),
        // Valid certificate config with PFX.
        ("testdata/azuread_good_certificate_pfx.yaml", ""),
        // Missing certificate client id.
        ("testdata/azuread_bad_certificate_missingclientid.yaml", "must provide an Azure Certificate client_id in the Azure AD config"),
        // Missing certificate tenant id.
        ("testdata/azuread_bad_certificate_missingtenantid.yaml", "must provide an Azure Certificate tenant_id in the Azure AD config"),
        // Missing certificate path.
        ("testdata/azuread_bad_certificate_missingpath.yaml", "must provide an Azure Certificate certificate_path in the Azure AD config"),
        // Invalid certificate client id.
        ("testdata/azuread_bad_certificate_invalidclientid.yaml", "the provided Azure Certificate client_id is invalid"),
        // Invalid config when both certificate and oauth is provided.
        ("testdata/azuread_bad_certificate_oauth.yaml", "cannot provide multiple authentication methods in the Azure AD config"),
    ];
    for (filename, err) in cases {
        let r = load_azure_ad_config(filename);
        if !err.is_empty() {
            let Err(e) = r else {
                panic!("Did not receive expected error unmarshaling bad azuread config");
            };
            assert_eq!(*err, e, "{filename}");
        } else {
            assert!(r.is_ok(), "{filename}: {:?}", r.err());
        }
    }
}

fn must_get_access_token(tp: &TokenProvider) -> String {
    tp.get_access_token().expect("getAccessToken")
}

fn get_token() -> AccessToken {
    AccessToken { token: discovery::azure::identity::new_uuid(), expires_on: SystemTime::now() + Duration::from_secs(10) }
}

#[test]
fn test_new_token_provider() {
    let cases: Vec<(&str, AzureAdConfig, &str)> = vec![
        ("invalid managed identity cloud", managed_identity_cfg("PublicAzure"), "Cloud is not specified or is incorrect: "),
        ("invalid oauth cloud", oauth_cfg("PublicAzure"), "Cloud is not specified or is incorrect: "),
        ("invalid SDK cloud", sdk_cfg("PublicAzure"), "Cloud is not specified or is incorrect: "),
        ("invalid workload identity cloud", workload_identity_cfg("PublicAzure"), "Cloud is not specified or is incorrect: "),
        ("valid managed identity", managed_identity_cfg("AzurePublic"), ""),
        ("valid oauth", oauth_cfg("AzurePublic"), ""),
        ("valid SDK", sdk_cfg("AzurePublic"), ""),
        ("valid workload identity", workload_identity_cfg("AzurePublic"), ""),
    ];

    // The subtests run in parallel, as with t.Parallel().
    let handles: Vec<_> = cases
        .into_iter()
        .map(|(name, cfg, err)| {
            std::thread::Builder::new()
                .name(name.to_string())
                .spawn(move || {
                    // Each subtest uses its own mock so cases can run concurrently.
                    let mock_cred = Arc::new(MockCredential::default());

                    if !err.is_empty() {
                        let r = new_token_provider(&cfg, mock_cred.clone());
                        let Err(actual_err) = r else {
                            panic!("{name}: expected an error and no token provider");
                        };
                        assert!(actual_err.contains(err), "{name}: {actual_err:?} does not contain {err:?}");
                        return;
                    }

                    let test_token = AccessToken { token: TEST_TOKEN_STRING.into(), expires_on: test_token_expiry() };

                    mock_cred.on(None, test_token, true);
                    mock_cred.on(None, get_token(), true);

                    let actual_token_provider = new_token_provider(&cfg, mock_cred.clone()).unwrap_or_else(|e| panic!("{name}: {e}"));

                    assert!(!must_get_access_token(&actual_token_provider).is_empty());

                    // Token set to refresh at half of the expiry time. The test tokens are set to expiry in 5s.
                    // Hence, the 4 seconds wait to check if the token is refreshed.
                    std::thread::sleep(Duration::from_secs(4));

                    assert!(!must_get_access_token(&actual_token_provider).is_empty());

                    mock_cred.assert_number_of_calls(2);
                    let access_token = must_get_access_token(&actual_token_provider);
                    assert_ne!(TEST_TOKEN_STRING, access_token);
                })
                .unwrap()
        })
        .collect();
    let mut failed = 0;
    for h in handles {
        if h.join().is_err() {
            failed += 1;
        }
    }
    assert_eq!(0, failed, "{failed} subtests failed");
}

#[test]
fn test_custom_scope_support() {
    let mock_credential = Arc::new(MockCredential::default());

    let cases: Vec<(&str, AzureAdConfig, &str)> = vec![
        ("Custom scope with OAuth", AzureAdConfig { scope: "https://custom-app.com/.default".into(), ..oauth_cfg("AzurePublic") }, "https://custom-app.com/.default"),
        ("Custom scope with Managed Identity", AzureAdConfig { scope: "https://monitor.azure.com//.default".into(), ..managed_identity_cfg("AzurePublic") }, "https://monitor.azure.com//.default"),
        ("Default scope fallback with OAuth", oauth_cfg("AzurePublic"), INGESTION_PUBLIC_AUDIENCE),
        ("Default scope fallback with China cloud", oauth_cfg("AzureChina"), INGESTION_CHINA_AUDIENCE),
    ];

    for (name, cfg, expected_scope) in cases {
        let test_token = AccessToken { token: TEST_TOKEN_STRING.into(), expires_on: test_token_expiry() };
        // Set up mock to capture the actual scopes used
        let es = expected_scope.to_string();
        mock_credential.on(Some(Box::new(move |o: &TokenRequestOptions| o.scopes.len() == 1 && o.scopes[0] == es)), test_token, true);

        let token_provider = new_token_provider(&cfg, mock_credential.clone()).unwrap_or_else(|e| panic!("{name}: {e}"));

        // Verify that the token provider uses the expected scope
        let token = token_provider.get_access_token().unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(TEST_TOKEN_STRING, token, "{name}");

        // Reset mock for next test
        mock_credential.reset();
    }
}

#[test]
fn test_workload_identity_token_file_path() {
    const ENV_TOKEN_PATH: &str = "/var/run/secrets/azure/wi/token/azure-identity-token";

    // (name, env value, env set, config token path, expected token path)
    let cases: &[(&str, &str, bool, &str, &str)] = &[
        ("AZURE_FEDERATED_TOKEN_FILE is honored when token_file_path is unset", ENV_TOKEN_PATH, true, "", ENV_TOKEN_PATH),
        ("falls back to the default path when the environment variable is unset", "", false, "", DEFAULT_WORKLOAD_IDENTITY_TOKEN_PATH),
        ("an explicit token_file_path takes precedence over the environment variable", ENV_TOKEN_PATH, true, "/custom/token/path", "/custom/token/path"),
    ];

    let prev = std::env::var(AZURE_FEDERATED_TOKEN_FILE_ENV_VAR).ok();
    for (name, env_value, env_set, config_token_path, expected_token_path) in cases {
        // SAFETY (test only): no other test in this binary reads AZURE_FEDERATED_TOKEN_FILE while this runs; the
        // configs of the other tests name the token path.
        if *env_set {
            unsafe { std::env::set_var(AZURE_FEDERATED_TOKEN_FILE_ENV_VAR, env_value) };
        } else {
            unsafe { std::env::remove_var(AZURE_FEDERATED_TOKEN_FILE_ENV_VAR) };
        }

        let mut cfg = AzureAdConfig {
            cloud: "AzurePublic".into(),
            workload_identity: Some(WorkloadIdentityConfig { client_id: DUMMY_CLIENT_ID.into(), tenant_id: DUMMY_TENANT_ID.into(), token_file_path: config_token_path.to_string() }),
            ..Default::default()
        };

        assert_eq!(Ok(()), cfg.validate(), "{name}");
        assert_eq!(*expected_token_path, cfg.workload_identity.as_ref().unwrap().token_file_path, "{name}");
    }
    match prev {
        Some(v) => unsafe { std::env::set_var(AZURE_FEDERATED_TOKEN_FILE_ENV_VAR, v) },
        None => unsafe { std::env::remove_var(AZURE_FEDERATED_TOKEN_FILE_ENV_VAR) },
    }
}
