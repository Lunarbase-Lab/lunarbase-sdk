impl IndexerConfig {
    /// Reads one TOML configuration file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        Ok(toml::from_str(&contents)?)
    }

    /// Parses addresses, code hash, socket address, and runtime bounds.
    pub fn validate(self) -> Result<ValidatedConfig, ConfigError> {
        let network = Network::from(self.network);
        let core = parse_address(&self.core, "core")?;
        let expected_runtime_code_hash = parse_hash(
            &self.expected_runtime_code_hash,
            "expected_runtime_code_hash",
        )?;
        let explicit_lane_assets = self
            .explicit_lane_assets
            .iter()
            .map(|value| parse_address(value, "explicit_lane_assets"))
            .collect::<Result<Vec<_>, _>>()?;
        let eager_routers = self
            .eager_routers
            .iter()
            .map(|value| parse_address(value, "eager_routers"))
            .collect::<Result<Vec<_>, _>>()?;
        let bind = self
            .bind
            .parse::<SocketAddr>()
            .map_err(|error| ConfigError::Invalid {
                field: "bind",
                detail: error.to_string(),
            })?;
        if self.runtime.buffer_capacity == 0 || self.runtime.reconnect_delay_milliseconds == 0 {
            return Err(ConfigError::Invalid {
                field: "runtime",
                detail: "buffer and reconnect delay must be non-zero".into(),
            });
        }
        if self.transport.max_frame_bytes == 0 || self.transport.reorder_capacity == 0 {
            return Err(ConfigError::Invalid {
                field: "transport",
                detail: "frame and reorder bounds must be non-zero".into(),
            });
        }
        if self.shutdown.timeout_seconds == 0 {
            return Err(ConfigError::Invalid {
                field: "shutdown.timeout_seconds",
                detail: "shutdown timeout must be non-zero".into(),
            });
        }
        if self.redis.io_timeout_milliseconds == 0 {
            return Err(ConfigError::Invalid {
                field: "redis.io_timeout_milliseconds",
                detail: "Redis I/O timeout must be non-zero".into(),
            });
        }
        if self.redis.io_timeout_milliseconds.saturating_mul(2)
            > self.shutdown.timeout_seconds.saturating_mul(1_000)
        {
            return Err(ConfigError::Invalid {
                field: "redis.io_timeout_milliseconds",
                detail: "two bounded Redis attempts must fit inside the shutdown timeout".into(),
            });
        }
        let writer_lease_enabled = self.writer_lease.enabled.unwrap_or(self.redis.enabled);
        if writer_lease_enabled && !self.redis.enabled {
            return Err(ConfigError::Invalid {
                field: "writer_lease.enabled",
                detail: "writer lease requires Redis persistence".into(),
            });
        }
        if self.writer_lease.ttl_milliseconds == 0
            || self.writer_lease.renew_interval_milliseconds == 0
            || self.writer_lease.retry_interval_milliseconds == 0
        {
            return Err(ConfigError::Invalid {
                field: "writer_lease",
                detail: "TTL, renew interval, and retry interval must be non-zero".into(),
            });
        }
        if self
            .writer_lease
            .renew_interval_milliseconds
            .saturating_add(self.redis.io_timeout_milliseconds.saturating_mul(2))
            >= self.writer_lease.ttl_milliseconds
        {
            return Err(ConfigError::Invalid {
                field: "writer_lease",
                detail: "lease TTL must exceed the renew interval plus two Redis I/O timeouts"
                    .into(),
            });
        }
        if self.alerts.poll_interval_seconds == 0
            || self.alerts.not_ready_after_seconds == 0
            || self.alerts.repeat_interval_seconds == 0
            || self.alerts.request_timeout_seconds == 0
        {
            return Err(ConfigError::Invalid {
                field: "alerts",
                detail: "all alert timing bounds must be non-zero".into(),
            });
        }
        if self.alerts.request_timeout_seconds > self.shutdown.timeout_seconds {
            return Err(ConfigError::Invalid {
                field: "alerts.request_timeout_seconds",
                detail: "alert request timeout cannot exceed the shutdown timeout".into(),
            });
        }
        let redis = RedisConfig {
            url: self.redis.url,
            stream_max_len: self.redis.stream_max_len,
            dedup_ttl_seconds: self.redis.dedup_ttl_seconds,
            checkpoint_interval_updates: self.redis.checkpoint_interval_updates,
        };
        let deployment = DeploymentConfig {
            network,
            chain_id: self.chain_id.unwrap_or_else(|| network.default_chain_id()),
            core,
            deployment_block: self.deployment_block,
            expected_runtime_code_hash,
            contract_compatibility_version: self.contract_compatibility_version,
            http_rpc_url: self.http_rpc_url,
            realtime_source: self.realtime_url,
            redis,
            explicit_lane_assets,
            eager_routers,
        };
        deployment
            .validate()
            .map_err(|error| ConfigError::Invalid {
                field: "deployment",
                detail: error.to_string(),
            })?;
        if self.snapshot_tag != "latest"
            && self.snapshot_tag != "safe"
            && self.snapshot_tag != "finalized"
        {
            return Err(ConfigError::Invalid {
                field: "snapshot_tag",
                detail: "expected latest, safe, or finalized".into(),
            });
        }
        let webhook_url = std::env::var("LUNARBASE_ALERT_WEBHOOK_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                let value = self.alerts.webhook_url.trim();
                (!value.is_empty()).then(|| value.to_owned())
            });
        if let Some(webhook_url) = &webhook_url {
            let parsed =
                reqwest::Url::parse(webhook_url).map_err(|error| ConfigError::Invalid {
                    field: "alerts.webhook_url",
                    detail: error.to_string(),
                })?;
            if parsed.scheme() != "http" && parsed.scheme() != "https" {
                return Err(ConfigError::Invalid {
                    field: "alerts.webhook_url",
                    detail: "expected an http or https URL".into(),
                });
            }
        }
        Ok(ValidatedConfig {
            deployment,
            snapshot_tag: self.snapshot_tag,
            bind,
            runtime: self.runtime,
            redis_enabled: self.redis.enabled,
            redis_io_timeout: Duration::from_millis(self.redis.io_timeout_milliseconds),
            writer_lease: ValidatedWriterLeaseConfig {
                enabled: writer_lease_enabled,
                owner: writer_lease_owner(&self.writer_lease.owner),
                ttl: Duration::from_millis(self.writer_lease.ttl_milliseconds),
                renew_interval: Duration::from_millis(
                    self.writer_lease.renew_interval_milliseconds,
                ),
                retry_interval: Duration::from_millis(
                    self.writer_lease.retry_interval_milliseconds,
                ),
            },
            transport: self.transport,
            shutdown_timeout: Duration::from_secs(self.shutdown.timeout_seconds),
            alerts: ValidatedAlertsConfig {
                enabled: self.alerts.enabled,
                webhook_url,
                poll_interval: Duration::from_secs(self.alerts.poll_interval_seconds),
                not_ready_after: Duration::from_secs(self.alerts.not_ready_after_seconds),
                repeat_interval: Duration::from_secs(self.alerts.repeat_interval_seconds),
                request_timeout: Duration::from_secs(self.alerts.request_timeout_seconds),
            },
        })
    }
}

