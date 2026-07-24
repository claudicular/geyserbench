use crate::proto::geyser::CommitmentLevel;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigToml {
    pub config: Config,
    pub endpoint: Vec<Endpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validator_map: Option<ValidatorMapSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub influx_sink: Option<InfluxSinkSettings>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub transactions: i32,
    pub account: String,
    pub commitment: ArgsCommitment,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<u64>,
}

/// Validator location input in the PersistedRttMap v1 format (see docs/validator-map.md).
/// `source` is a filesystem path or an http(s) URL.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ValidatorMapSettings {
    pub source: String,
    #[serde(default = "default_icmp_threshold_us")]
    pub rtt_icmp_threshold_us: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rtt_quic_threshold_us: Option<u64>,
    #[serde(default = "default_map_refresh_secs")]
    pub refresh_secs: u64,
}

impl ValidatorMapSettings {
    pub fn quic_threshold_us(&self) -> u64 {
        self.rtt_quic_threshold_us
            .unwrap_or(self.rtt_icmp_threshold_us + 1_000)
    }
}

fn default_icmp_threshold_us() -> u64 {
    5_000
}

fn default_map_refresh_secs() -> u64 {
    300
}

/// Long-running metrics export: per-(leader, endpoint) window counters written
/// to InfluxDB v2 as line protocol.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct InfluxSinkSettings {
    pub url: String,
    pub org: String,
    pub bucket: String,
    pub token: String,
    #[serde(default = "default_sink_measurement")]
    pub measurement: String,
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
}

fn default_sink_measurement() -> String {
    "geyserbench_vs_winner".to_string()
}

fn default_flush_interval_secs() -> u64 {
    15
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Endpoint {
    pub name: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x_token: Option<String>,
    pub kind: EndpointKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub influx_org: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub influx_bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub influx_stage: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EndpointKind {
    Yellowstone,
    #[serde(rename = "yellowstone_tx_accounts")]
    YellowstoneTxAccounts,
    Arpc,
    Thor,
    Shredstream,
    Shreder,
    Jetstream,
    Influxdb,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

impl ArgsCommitment {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArgsCommitment::Processed => "processed",
            ArgsCommitment::Confirmed => "confirmed",
            ArgsCommitment::Finalized => "finalized",
        }
    }
}

impl EndpointKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EndpointKind::Yellowstone => "yellowstone",
            EndpointKind::YellowstoneTxAccounts => "yellowstone_tx_accounts",
            EndpointKind::Arpc => "arpc",
            EndpointKind::Thor => "thor",
            EndpointKind::Shredstream => "shredstream",
            EndpointKind::Shreder => "shreder",
            EndpointKind::Jetstream => "jetstream",
            EndpointKind::Influxdb => "influxdb",
        }
    }
}

impl ConfigToml {
    pub fn load(path: &str) -> Result<Self> {
        let content =
            fs::read_to_string(path).with_context(|| format!("Failed to read config {}", path))?;
        let config = toml::from_str(&content).map_err(|err| anyhow!(err))?;
        Ok(config)
    }

    pub fn create_default(path: &str) -> Result<Self> {
        let default_config = ConfigToml {
            config: Config {
                transactions: 1000,
                account: "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string(),
                commitment: ArgsCommitment::Processed,
                rpc_url: None,
                duration_secs: None,
            },
            endpoint: vec![
                Endpoint {
                    name: "grpc".to_string(),
                    url: "http://fra.corvus-labs.io:10101".to_string(),
                    x_token: None,
                    kind: EndpointKind::Yellowstone,
                    influx_org: None,
                    influx_bucket: None,
                    influx_stage: None,
                },
                Endpoint {
                    name: "arpc".to_string(),
                    url: "http://fra.corvus-labs.io:20202".to_string(),
                    x_token: None,
                    kind: EndpointKind::Arpc,
                    influx_org: None,
                    influx_bucket: None,
                    influx_stage: None,
                },
            ],
            validator_map: None,
            influx_sink: None,
        };

        let toml_string = toml::to_string_pretty(&default_config)
            .context("Failed to serialize default config")?;
        fs::write(path, toml_string)
            .with_context(|| format!("Failed to write default config {}", path))?;

        Ok(default_config)
    }

    pub fn load_or_create(path: &str) -> Result<Self> {
        if Path::new(path).exists() {
            Self::load(path)
        } else {
            Self::create_default(path)
        }
    }
}
