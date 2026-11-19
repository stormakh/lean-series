//! Port of `web/api/testhelpers/assertions.go`: chained assertions on a [`Response`].

use json::Json;

use super::request::Response;

/// Go `getJSONPath`: a value by a simple path like "$.data.groups". Array indexing is not supported (as in Go).
/// A JSON null counts as missing (Go's decoded `nil`).
pub fn get_json_path<'a>(data: &'a Json, path: &str) -> Option<&'a Json> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    if path.is_empty() {
        return Some(data);
    }
    let mut current = data;
    for part in path.split('.') {
        if part.contains('[') {
            return None;
        }
        match current {
            Json::Obj(_) => current = current.get(part)?,
            _ => return None,
        }
    }
    match current {
        Json::Null => None,
        v => Some(v),
    }
}

/// Go `any(expected)` compared with `require.Equal` / `slices.Contains`: a JSON string.
fn is_str(v: &Json, s: &str) -> bool {
    matches!(v, Json::Str(x) if x == s)
}

impl Response {
    fn json_or_fail(&self) -> &Json {
        self.json
            .as_ref()
            .unwrap_or_else(|| panic!("response body is not JSON: {}", self.body))
    }

    fn require_path(&self, path: &str) -> &Json {
        get_json_path(self.json_or_fail(), path)
            .unwrap_or_else(|| panic!("JSON path {path:?} does not exist in {}", self.body))
    }

    fn require_array(&self, path: &str) -> &Vec<Json> {
        match self.require_path(path) {
            Json::Arr(a) => a,
            _ => panic!("JSON path {path:?} is not an array in {}", self.body),
        }
    }

    /// Go `RequireSuccess`.
    pub fn require_success(&self) -> &Self {
        let status = self.json_or_fail().get("status");
        assert!(
            status.is_some_and(|s| is_str(s, "success")),
            "expected status to be 'success': {}",
            self.body
        );
        self
    }

    /// Go `RequireError`.
    pub fn require_error(&self) -> &Self {
        let status = self.json_or_fail().get("status");
        assert!(
            status.is_some_and(|s| is_str(s, "error")),
            "expected status to be 'error': {}",
            self.body
        );
        self
    }

    /// Go `RequireStatusCode`.
    pub fn require_status_code(&self, expected_code: u16) -> &Self {
        assert_eq!(
            expected_code, self.status_code,
            "unexpected HTTP status code: {}",
            self.body
        );
        self
    }

    /// Go `RequireJSONPathExists`.
    pub fn require_json_path_exists(&self, path: &str) -> &Self {
        self.require_path(path);
        self
    }

    /// Go `RequireJSONPathNotExists`.
    pub fn require_json_path_not_exists(&self, path: &str) -> &Self {
        let v = get_json_path(self.json_or_fail(), path);
        assert!(
            v.is_none(),
            "JSON path {path:?} should not exist but was found: {}",
            self.body
        );
        self
    }

    /// Go `RequireEquals` with a string value.
    pub fn require_equals(&self, path: &str, expected: &str) -> &Self {
        let v = self.require_path(path);
        assert!(
            is_str(v, expected),
            "JSON path {path:?} has unexpected value {v:?}, want {expected:?}"
        );
        self
    }

    /// Go `RequireJSONArray`.
    pub fn require_json_array(&self, path: &str) -> &Self {
        self.require_array(path);
        self
    }

    /// Go `RequireLenAtLeast`.
    pub fn require_len_at_least(&self, path: &str, min_len: usize) -> &Self {
        let a = self.require_array(path);
        assert!(
            a.len() >= min_len,
            "JSON path {path:?} has fewer than {min_len} elements: {}",
            self.body
        );
        self
    }

    /// Go `RequireArrayContains` with a string value.
    pub fn require_array_contains(&self, path: &str, expected: &str) -> &Self {
        let a = self.require_array(path);
        assert!(
            a.iter().any(|v| is_str(v, expected)),
            "JSON path {path:?} does not contain expected value {expected:?}: {}",
            self.body
        );
        self
    }

    /// Go `RequireSome`.
    pub fn require_some(&self, path: &str, predicate: impl Fn(&Json) -> bool) -> &Self {
        let a = self.require_array(path);
        assert!(
            a.iter().any(predicate),
            "no element in JSON path {path:?} satisfies the predicate: {}",
            self.body
        );
        self
    }
}
