//! Credential storage abstraction
//!
//! Provides a trait for credential storage with implementations for:
//! - KeyringStore: Uses the system keychain (macOS Keychain, Windows Credential Manager, etc.)
//! - MockStore: In-memory storage for testing

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Errors that can occur during credential operations
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("Credential not found")]
    NotFound,
    #[error("Keyring error: {0}")]
    Keyring(String),
}

/// Trait for credential storage backends
pub trait CredentialStore: Send + Sync {
    /// Get a credential by service and user
    fn get(&self, service: &str, user: &str) -> Result<String, CredentialError>;

    /// Set a credential
    fn set(&self, service: &str, user: &str, password: &str) -> Result<(), CredentialError>;

    /// Delete a credential
    fn delete(&self, service: &str, user: &str) -> Result<(), CredentialError>;
}

/// Real keyring-based credential store
pub struct KeyringStore;

impl KeyringStore {
    pub fn new() -> Self {
        Self
    }
}

impl Default for KeyringStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CredentialStore for KeyringStore {
    fn get(&self, service: &str, user: &str) -> Result<String, CredentialError> {
        let entry = keyring::Entry::new(service, user)
            .map_err(|e| CredentialError::Keyring(e.to_string()))?;
        entry.get_password().map_err(|e| match e {
            keyring::Error::NoEntry => CredentialError::NotFound,
            _ => CredentialError::Keyring(e.to_string()),
        })
    }

    fn set(&self, service: &str, user: &str, password: &str) -> Result<(), CredentialError> {
        let entry = keyring::Entry::new(service, user)
            .map_err(|e| CredentialError::Keyring(e.to_string()))?;
        entry
            .set_password(password)
            .map_err(|e| CredentialError::Keyring(e.to_string()))
    }

    fn delete(&self, service: &str, user: &str) -> Result<(), CredentialError> {
        let entry = keyring::Entry::new(service, user)
            .map_err(|e| CredentialError::Keyring(e.to_string()))?;
        entry.delete_credential().map_err(|e| match e {
            keyring::Error::NoEntry => CredentialError::NotFound,
            _ => CredentialError::Keyring(e.to_string()),
        })
    }
}

/// In-memory credential store for testing
#[derive(Clone, Default)]
pub struct MockStore {
    store: Arc<Mutex<HashMap<(String, String), String>>>,
}

impl MockStore {
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a mock store with pre-populated credentials
    pub fn with_credentials(credentials: Vec<(&str, &str, &str)>) -> Self {
        let store = Self::new();
        for (service, user, password) in credentials {
            store.set(service, user, password).unwrap();
        }
        store
    }
}

impl CredentialStore for MockStore {
    fn get(&self, service: &str, user: &str) -> Result<String, CredentialError> {
        let store = self.store.lock().unwrap();
        store
            .get(&(service.to_string(), user.to_string()))
            .cloned()
            .ok_or(CredentialError::NotFound)
    }

    fn set(&self, service: &str, user: &str, password: &str) -> Result<(), CredentialError> {
        let mut store = self.store.lock().unwrap();
        store.insert(
            (service.to_string(), user.to_string()),
            password.to_string(),
        );
        Ok(())
    }

    fn delete(&self, service: &str, user: &str) -> Result<(), CredentialError> {
        let mut store = self.store.lock().unwrap();
        store
            .remove(&(service.to_string(), user.to_string()))
            .map(|_| ())
            .ok_or(CredentialError::NotFound)
    }
}

/// Get the default credential store (keyring for production)
pub fn default_store() -> Arc<dyn CredentialStore> {
    Arc::new(KeyringStore::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_store_get_set() {
        let store = MockStore::new();
        store.set("service", "user", "password123").unwrap();
        assert_eq!(store.get("service", "user").unwrap(), "password123");
    }

    #[test]
    fn test_mock_store_not_found() {
        let store = MockStore::new();
        assert!(matches!(
            store.get("service", "user"),
            Err(CredentialError::NotFound)
        ));
    }

    #[test]
    fn test_mock_store_delete() {
        let store = MockStore::new();
        store.set("service", "user", "password123").unwrap();
        store.delete("service", "user").unwrap();
        assert!(matches!(
            store.get("service", "user"),
            Err(CredentialError::NotFound)
        ));
    }

    #[test]
    fn test_mock_store_with_credentials() {
        let store = MockStore::with_credentials(vec![
            ("svc1", "user1", "pass1"),
            ("svc2", "user2", "pass2"),
        ]);
        assert_eq!(store.get("svc1", "user1").unwrap(), "pass1");
        assert_eq!(store.get("svc2", "user2").unwrap(), "pass2");
    }
}
