fn writer_lease_owner(configured: &str) -> String {
    if let Ok(owner) = std::env::var("LUNARBASE_WRITER_ID") {
        if !owner.trim().is_empty() {
            return owner;
        }
    }
    if !configured.trim().is_empty() {
        return configured.to_owned();
    }
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".into());
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{host}:{}:{started}", std::process::id())
}

fn parse_address(value: &str, field: &'static str) -> Result<Address, ConfigError> {
    Address::from_hex(value).map_err(|error| ConfigError::Invalid {
        field,
        detail: error.to_string(),
    })
}

fn parse_hash(value: &str, field: &'static str) -> Result<[u8; 32], ConfigError> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 64 {
        return Err(ConfigError::Invalid {
            field,
            detail: "expected a 32-byte hexadecimal value".into(),
        });
    }
    let mut result = [0; 32];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(|_| {
            ConfigError::Invalid {
                field,
                detail: "expected a 32-byte hexadecimal value".into(),
            }
        })?;
    }
    Ok(result)
}

fn default_contract_compatibility() -> String {
    MATH_COMPATIBILITY_VERSION.into()
}

fn default_snapshot_tag() -> String {
    "finalized".into()
}

fn default_bind() -> String {
    "127.0.0.1:8080".into()
}

