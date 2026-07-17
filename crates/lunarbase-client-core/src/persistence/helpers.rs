fn duration_milliseconds(duration: Duration) -> redis::RedisResult<u64> {
    if duration.is_zero() {
        return Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "writer lease TTL must be non-zero",
        )));
    }
    u64::try_from(duration.as_millis()).map_err(|_| {
        redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "writer lease TTL exceeds Redis PX range",
        ))
    })
}

fn managed_redis_error(error: String) -> redis::RedisError {
    redis::RedisError::from((
        redis::ErrorKind::IoError,
        "managed Redis operation failed",
        error,
    ))
}

pub(crate) fn update_dedup_key(namespace: &RedisNamespace, update: &ChainUpdate) -> String {
    format!("lb:{{{}}}:dedup:{}", namespace.tag, update_identity(update))
}

fn update_identity(update: &ChainUpdate) -> String {
    let (kind, cursor) = match update {
        ChainUpdate::Head(cursor) => ("head", Some(cursor)),
        ChainUpdate::Log(log) => ("log", Some(&log.cursor)),
        ChainUpdate::Reorg { new_head, .. } => ("reorg", Some(new_head)),
        ChainUpdate::Gap { cursor, .. } => ("gap", cursor.as_ref()),
        ChainUpdate::SourceHealth { healthy, .. } => {
            return format!("health:{}", u8::from(*healthy));
        }
    };
    cursor.map_or_else(
        || kind.to_owned(),
        |cursor| {
            format!(
                "{kind}:{}:{}:{}:{}",
                cursor.block_number,
                cursor.transaction_index.unwrap_or_default(),
                cursor.log_index.unwrap_or_default(),
                cursor.source_sequence.unwrap_or_default()
            )
        },
    )
}

