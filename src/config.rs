use std::path::PathBuf;

pub struct Config {
    pub port: u16,
    pub data_dir: PathBuf,
    pub api_key: String,
    /// Exact-match Origin allowlist for browser requests (lowercased).
    /// Empty list means requests carrying an Origin header are rejected.
    pub allowed_origins: Vec<String>,
    pub compact_interval_secs: u64,
    pub ttl_days: i64,
    /// Lowercased property/context keys stripped on ingest (PII guard).
    pub property_denylist: Vec<String>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let api_key = std::env::var("PULSE_API_KEY")
            .map_err(|_| anyhow::anyhow!("PULSE_API_KEY is required"))?;
        anyhow::ensure!(
            api_key.trim().len() >= 16,
            "PULSE_API_KEY must be at least 16 characters"
        );
        Ok(Self {
            port: env_or("PULSE_PORT", "8080").parse()?,
            data_dir: PathBuf::from(env_or("PULSE_DATA_DIR", "./data")),
            api_key: api_key.trim().to_string(),
            allowed_origins: csv(&env_or("PULSE_ALLOWED_ORIGINS", "")),
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
