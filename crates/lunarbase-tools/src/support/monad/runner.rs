/// Monitors parser sequencing/commitments and repeatedly compares indexer
/// quotes with direct Solidity `eth_call` results.
pub async fn run(arguments: MonadArguments) -> Result<(), MonadError> {
    if arguments.duration_seconds == 0 || arguments.sample_interval_milliseconds == 0 {
        return Err(MonadError::Validation(
            "duration and sample interval must be non-zero".into(),
        ));
    }
    let vectors = match &arguments.vectors {
        Some(path) => serde_json::from_slice::<Vec<ValidationVector>>(&std::fs::read(path)?)?,
        None => Vec::new(),
    };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    require_success(&http, &arguments.parser_ready_url, "Monad parser").await?;
    require_success(
        &http,
        &format!(
            "{}/health/ready",
            arguments.indexer_url.trim_end_matches('/')
        ),
        "LunarBase indexer",
    )
    .await?;

    let metrics_before = metrics(&http, &arguments.indexer_url).await;
    let (stop, parser_stop) = watch::channel(false);
    let parser_url = arguments.parser_ws_url.clone();
    let parser_task = tokio::spawn(async move { monitor_parser(&parser_url, parser_stop).await });
    let started = Instant::now();
    let mut ticker = interval(Duration::from_millis(
        arguments.sample_interval_milliseconds,
    ));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut samples = 0u64;
    let mut parser_ready_failures = 0u64;
    let mut indexer_readiness_failures = 0u64;
    let mut rpc_failures = 0u64;
    let mut maximum_lag = 0u64;
    let mut quote_comparisons = 0u64;
    let mut quote_mismatches = 0u64;

    while started.elapsed() < Duration::from_secs(arguments.duration_seconds) {
        ticker.tick().await;
        samples = samples.saturating_add(1);
        if !is_success(&http, &arguments.parser_ready_url).await {
            parser_ready_failures = parser_ready_failures.saturating_add(1);
        }
        let indexer_health_url = format!(
            "{}/health/ready",
            arguments.indexer_url.trim_end_matches('/')
        );
        let indexer_health = http.get(&indexer_health_url).send().await;
        let indexed_block = match indexer_health {
            Ok(response) if response.status().is_success() => {
                response.json::<Value>().await.ok().and_then(|value| {
                    value
                        .pointer("/cursor/blockNumber")?
                        .as_str()?
                        .parse::<u64>()
                        .ok()
                })
            }
            _ => {
                indexer_readiness_failures = indexer_readiness_failures.saturating_add(1);
                None
            }
        };
        match rpc_block_number(&http, &arguments.rpc_url).await {
            Ok(rpc_block) => {
                if let Some(indexed_block) = indexed_block {
                    maximum_lag = maximum_lag.max(rpc_block.saturating_sub(indexed_block));
                    if indexed_block > rpc_block {
                        quote_mismatches = quote_mismatches.saturating_add(1);
                    }
                }
            }
            Err(_) => rpc_failures = rpc_failures.saturating_add(1),
        }

        for vector in &vectors {
            let Some(solidity) = &vector.solidity else {
                continue;
            };
            quote_comparisons = quote_comparisons.saturating_add(1);
            if !compare_vector(&http, &arguments, vector, solidity).await {
                quote_mismatches = quote_mismatches.saturating_add(1);
            }
        }
    }

    let _ = stop.send(true);
    let parser = timeout(Duration::from_secs(5), parser_task)
        .await
        .map_err(|_| MonadError::Validation("parser monitor did not stop".into()))?
        .map_err(|error| MonadError::Validation(format!("parser monitor panicked: {error}")))??;
    let metrics_after = metrics(&http, &arguments.indexer_url).await;
    let clean = parser.explicit_gaps == 0
        && parser.sequence_regressions == 0
        && parser.commitment_regressions == 0
        && parser_ready_failures == 0
        && indexer_readiness_failures == 0
        && rpc_failures == 0
        && quote_mismatches == 0;
    let report = MonadReport {
        duration_seconds: started.elapsed().as_secs_f64(),
        samples,
        parser,
        parser_ready_failures,
        indexer_readiness_failures,
        rpc_failures,
        maximum_indexer_lag_blocks: maximum_lag,
        quote_comparisons,
        quote_mismatches,
        reconnects_delta: metric_delta(
            &metrics_before,
            &metrics_after,
            "lunarbase_source_reconnects_total",
        ),
        gaps_delta: metric_delta(
            &metrics_before,
            &metrics_after,
            "lunarbase_source_gaps_total",
        ),
        recoveries_delta: metric_delta(
            &metrics_before,
            &metrics_after,
            "lunarbase_recoveries_total",
        ),
        recovery_failures_delta: metric_delta(
            &metrics_before,
            &metrics_after,
            "lunarbase_recovery_failures_total",
        ),
        status: if clean { "ok" } else { "failed" },
    };
    let serialized = serde_json::to_string_pretty(&report)?;
    println!("{serialized}");
    if let Some(path) = arguments.report {
        std::fs::write(path, &serialized)?;
    }
    if !clean {
        return Err(MonadError::Validation(
            "Monad live validation reported sequencing, readiness, RPC, or quote mismatches".into(),
        ));
    }
    Ok(())
}

