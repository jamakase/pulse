use std::path::PathBuf;

pub struct Config {
    pub port: u16,
    pub data_dir: PathBuf,
    /// Admin key: MCP + erasure ONLY. Lives with the operator, never ships
    /// to any app — it cannot even ingest.
    pub admin_key: String,
    /// Secret per-product server keys (`product:key,…`). Append-only; events
    /// may claim source='server' — the "nobody made these up" channel.
    pub server_keys: Vec<ProductKey>,
    /// Public per-product client keys (`product:key,…`). Append-only; source
    /// is forced to 'client'. Safe to ship in browser JS — the origin
    /// allowlist is the real gate there, and losing one costs nothing.
    pub client_keys: Vec<ProductKey>,
    /// Exact-match Origin allowlist for browser requests (lowercased).
    /// Empty list means requests carrying an Origin header are rejected.
    pub allowed_origins: Vec<String>,
    /// Host-header allowlist for the MCP transport (DNS-rebinding guard).
    /// Empty = accept any Host: pulse sits behind a reverse proxy and every
    /// request is bearer-authenticated, so rebinding gains nothing here.
    pub allowed_hosts: Vec<String>,
    pub compact_interval_secs: u64,
    pub ttl_days: i64,
    /// Lowercased property/context keys stripped on ingest (PII guard).
    pub property_denylist: Vec<String>,
}

pub struct ProductKey {
    pub product: String,
    pub key: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let admin_key = std::env::var("PULSE_ADMIN_KEY")
            .map_err(|_| anyhow::anyhow!("PULSE_ADMIN_KEY is required"))?;
        anyhow::ensure!(
            admin_key.trim().len() >= 16,
            "PULSE_ADMIN_KEY must be at least 16 characters"
        );
        Ok(Self {
            port: env_or("PULSE_PORT", "8080").parse()?,
            data_dir: PathBuf::from(env_or("PULSE_DATA_DIR", "./data")),
            admin_key: admin_key.trim().to_string(),
            server_keys: parse_product_keys(&env_or("PULSE_SERVER_KEYS", ""))?,
            client_keys: parse_product_keys(&env_or("PULSE_CLIENT_KEYS", ""))?,
            allowed_origins: csv(&env_or("PULSE_ALLOWED_ORIGINS", "")),
            allowed_hosts: csv(&env_or("PULSE_ALLOWED_HOSTS", "")),
            compact_interval_secs: env_or("PULSE_COMPACT_INTERVAL_SECS", "60").parse()?,
            ttl_days: env_or("PULSE_TTL_DAYS", "730").parse()?,
            property_denylist: csv(&env_or(
                "PULSE_PROPERTY_DENYLIST",
                "email,phone,name,first_name,last_name,password,token",
            )),
        })
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    pub fn events_dir(&self) -> PathBuf {
        self.data_dir.join("events")
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .collect()
}

/// `"myapp:ps_abc…,otherapp:ps_def…"`
fn parse_product_keys(s: &str) -> anyhow::Result<Vec<ProductKey>> {
    let mut keys = Vec::new();
    for pair in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (product, key) = pair
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("product key entries must be product:key"))?;
        let (product, key) = (product.trim().to_string(), key.trim().to_string());
        anyhow::ensure!(
            !product.is_empty()
                && product
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "invalid product '{product}' in product keys"
        );
        anyhow::ensure!(
            key.len() >= 16,
            "key for '{product}' must be at least 16 characters"
        );
        keys.push(ProductKey { product, key });
    }
    Ok(keys)
}
