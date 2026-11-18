//! Secret sources (http_config.go SecretReader): inline text, a file read on every use, or a secret manager ref.
use std::sync::Arc;

use yaml::Secret;

/// Secret data by name (config.SecretManager).
pub trait SecretManager: Send + Sync {
    fn fetch(&self, secret_ref: &str) -> Result<String, String>;
}

pub trait SecretReader: Send + Sync {
    fn fetch(&self) -> Result<String, String>;
    fn description(&self) -> String;
    fn immutable(&self) -> bool;
}

pub struct InlineSecret(pub String);
impl SecretReader for InlineSecret {
    fn fetch(&self) -> Result<String, String> {
        Ok(self.0.clone())
    }
    fn description(&self) -> String {
        "inline".to_string()
    }
    fn immutable(&self) -> bool {
        true
    }
}

/// Reads the file on every fetch and trims spaces.
pub struct FileSecret(pub String);
impl SecretReader for FileSecret {
    fn fetch(&self) -> Result<String, String> {
        let b = crate::types::read_file(&self.0).map_err(|e| format!("unable to read file {}: {}", self.0, e))?;
        Ok(String::from_utf8_lossy(&b).trim().to_string())
    }
    fn description(&self) -> String {
        format!("file {}", self.0)
    }
    fn immutable(&self) -> bool {
        false
    }
}

pub struct RefSecret {
    pub secret_ref: String,
    pub manager: Arc<dyn SecretManager>,
}
impl SecretReader for RefSecret {
    fn fetch(&self) -> Result<String, String> {
        self.manager.fetch(&self.secret_ref)
    }
    fn description(&self) -> String {
        format!("ref {}", self.secret_ref)
    }
    fn immutable(&self) -> bool {
        false
    }
}

pub fn new_inline_secret(s: &str) -> Arc<dyn SecretReader> {
    Arc::new(InlineSecret(s.to_string()))
}
pub fn new_file_secret(path: &str) -> Arc<dyn SecretReader> {
    Arc::new(FileSecret(path.to_string()))
}

/// Go `toSecret`: the one source that is set, or None.
pub fn to_secret(manager: Option<&Arc<dyn SecretManager>>, text: &Secret, file: &str, secret_ref: &str) -> Result<Option<Arc<dyn SecretReader>>, String> {
    if !text.is_empty() {
        return Ok(Some(Arc::new(InlineSecret(text.0.clone()))));
    }
    if !file.is_empty() {
        return Ok(Some(Arc::new(FileSecret(file.to_string()))));
    }
    if !secret_ref.is_empty() {
        return match manager {
            None => Err("cannot use secret ref without manager".to_string()),
            Some(m) => Ok(Some(Arc::new(RefSecret { secret_ref: secret_ref.to_string(), manager: m.clone() }))),
        };
    }
    Ok(None)
}
