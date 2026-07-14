use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};

use crate::{Result, ShardCacheClientError};

/// Immutable TLS identity and trust configuration for SCNP connections.
pub struct ScnpTlsClientConfig {
    config: RwLock<Arc<ClientConfig>>,
    server_name: ServerName<'static>,
    mutual_tls: bool,
    ca_path: PathBuf,
    client_cert_path: Option<PathBuf>,
    client_key_path: Option<PathBuf>,
    next_reload_ms: AtomicU64,
    reload_successes: AtomicU64,
    reload_failures: AtomicU64,
}

impl fmt::Debug for ScnpTlsClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScnpTlsClientConfig")
            .field("server_name", &self.server_name)
            .field("mutual_tls", &self.mutual_tls)
            .finish_non_exhaustive()
    }
}

impl ScnpTlsClientConfig {
    /// Loads a CA bundle and optional mTLS identity from PEM files.
    pub fn from_pem_files(
        server_name: &str,
        ca_path: impl AsRef<Path>,
        client_cert_path: Option<&Path>,
        client_key_path: Option<&Path>,
    ) -> Result<Self> {
        let ca_path = ca_path.as_ref().to_path_buf();
        let client_cert_path = client_cert_path.map(Path::to_path_buf);
        let client_key_path = client_key_path.map(Path::to_path_buf);
        let (config, mutual_tls) = load_client_config(
            &ca_path,
            client_cert_path.as_deref(),
            client_key_path.as_deref(),
        )?;
        let server_name = ServerName::try_from(server_name.to_owned()).map_err(|error| {
            ShardCacheClientError::Config(format!("invalid SCNP TLS server name: {error}"))
        })?;
        Ok(Self {
            config: RwLock::new(config),
            server_name,
            mutual_tls,
            ca_path,
            client_cert_path,
            client_key_path,
            next_reload_ms: AtomicU64::new(now_millis().saturating_add(30_000)),
            reload_successes: AtomicU64::new(0),
            reload_failures: AtomicU64::new(0),
        })
    }

    #[doc(hidden)]
    pub fn rustls_config(&self) -> Arc<ClientConfig> {
        let now = now_millis();
        let next = self.next_reload_ms.load(Ordering::Acquire);
        if now >= next
            && self
                .next_reload_ms
                .compare_exchange(
                    next,
                    now.saturating_add(30_000),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            match load_client_config(
                &self.ca_path,
                self.client_cert_path.as_deref(),
                self.client_key_path.as_deref(),
            ) {
                Ok((config, _)) => {
                    *self.config.write().expect("SCNP TLS config lock poisoned") = config;
                    self.reload_successes.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    self.reload_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Arc::clone(&self.config.read().expect("SCNP TLS config lock poisoned"))
    }

    #[doc(hidden)]
    pub fn server_name(&self) -> ServerName<'static> {
        self.server_name.clone()
    }

    /// Returns successful and failed certificate reload attempts.
    pub fn reload_health(&self) -> (u64, u64) {
        (
            self.reload_successes.load(Ordering::Relaxed),
            self.reload_failures.load(Ordering::Relaxed),
        )
    }
}

fn now_millis() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

fn load_client_config(
    ca_path: &Path,
    client_cert_path: Option<&Path>,
    client_key_path: Option<&Path>,
) -> Result<(Arc<ClientConfig>, bool)> {
    let mut roots = RootCertStore::empty();
    let mut ca_reader = BufReader::new(File::open(ca_path)?);
    let certificates =
        rustls_pemfile::certs(&mut ca_reader).collect::<std::io::Result<Vec<_>>>()?;
    if certificates.is_empty() {
        return Err(ShardCacheClientError::Config(
            "SCNP TLS CA file contains no certificates".into(),
        ));
    }
    for certificate in certificates {
        roots.add(certificate).map_err(|error| {
            ShardCacheClientError::Config(format!("invalid SCNP TLS CA certificate: {error}"))
        })?;
    }

    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|error| {
                ShardCacheClientError::Config(format!("invalid SCNP TLS protocol config: {error}"))
            })?
            .with_root_certificates(roots);
    let (mut config, mutual_tls) = match (client_cert_path, client_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let mut cert_reader = BufReader::new(File::open(cert_path)?);
            let certs =
                rustls_pemfile::certs(&mut cert_reader).collect::<std::io::Result<Vec<_>>>()?;
            if certs.is_empty() {
                return Err(ShardCacheClientError::Config(
                    "SCNP TLS client certificate file contains no certificates".into(),
                ));
            }
            let mut key_reader = BufReader::new(File::open(key_path)?);
            let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
                ShardCacheClientError::Config(
                    "SCNP TLS client key file contains no private key".into(),
                )
            })?;
            let config = builder.with_client_auth_cert(certs, key).map_err(|error| {
                ShardCacheClientError::Config(format!("invalid SCNP TLS client identity: {error}"))
            })?;
            (config, true)
        }
        (None, None) => (builder.with_no_client_auth(), false),
        _ => {
            return Err(ShardCacheClientError::Config(
                "SCNP TLS client certificate and key must be configured together".into(),
            ));
        }
    };
    config.alpn_protocols = vec![b"scnp/2".to_vec()];
    Ok((Arc::new(config), mutual_tls))
}
