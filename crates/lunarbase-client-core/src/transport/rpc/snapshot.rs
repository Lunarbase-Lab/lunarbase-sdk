#[derive(Clone)]
pub struct RpcSnapshotProvider {
    rpc: RpcHttpClient,
    snapshot_tag: Arc<str>,
}

impl RpcSnapshotProvider {
    /// Creates a block-tagged snapshot provider (`finalized` or `latest`).
    pub fn new(rpc: RpcHttpClient, snapshot_tag: impl Into<String>) -> Self {
        Self {
            rpc,
            snapshot_tag: Arc::from(snapshot_tag.into()),
        }
    }

    /// Returns the RPC client used for snapshot calls.
    pub fn rpc(&self) -> &RpcHttpClient {
        &self.rpc
    }
}

#[async_trait]
impl SnapshotProvider for RpcSnapshotProvider {
    async fn snapshot(
        &self,
        config: &DeploymentConfig,
        lane_assets: &[Address],
        routers: &[Address],
    ) -> Result<BootstrapSnapshot, SourceError> {
        config.validate()?;
        let commitment = if self.snapshot_tag.as_ref() == "finalized" {
            Commitment::Finalized
        } else {
            Commitment::Canonical
        };
        let cursor = self
            .rpc
            .block_cursor(&self.snapshot_tag, config.chain_id, commitment)
            .await?;
        if cursor.block_number < config.deployment_block {
            return Err(SourceError::Unavailable(
                "snapshot block precedes deployment block".into(),
            ));
        }
        let code = self.rpc.get_code(config.core, &self.snapshot_tag).await?;
        let runtime_code_hash = keccak256(&code);
        if config.expected_runtime_code_hash != [0; 32]
            && runtime_code_hash != config.expected_runtime_code_hash
        {
            return Err(SourceError::Unavailable(
                "runtime code hash does not match deployment config".into(),
            ));
        }

        let assets = self
            .resolve_lane_assets(config, lane_assets, cursor.block_number)
            .await?;
        let cash = decode_address_word(
            &self
                .rpc
                .call_at(config.core, SELECTOR_CASH.into(), &self.snapshot_tag)
                .await?,
        )?;
        let blacklist_fee_multiplier = decode_word(
            &self
                .rpc
                .call_at(
                    config.core,
                    SELECTOR_BLACKLIST_FEE_MULTIPLIER.into(),
                    &self.snapshot_tag,
                )
                .await?,
            0,
        )?;
        let mut state = QuoteState {
            cash,
            blacklist_fee_multiplier,
            ..Default::default()
        };

        for asset in &assets {
            let lane_words = decode_words(
                &self
                    .rpc
                    .call_at(
                        config.core,
                        selector_address(SELECTOR_LANE, *asset),
                        &self.snapshot_tag,
                    )
                    .await?,
                5,
            )?;
            let reserve_words = decode_words(
                &self
                    .rpc
                    .call_at(
                        config.core,
                        selector_address(SELECTOR_RESERVES, *asset),
                        &self.snapshot_tag,
                    )
                    .await?,
                5,
            )?;
            let block_delay = u8::try_from(lane_words[3]).map_err(|_| {
                SourceError::Unavailable("lane blockDelay does not fit uint8".into())
            })?;
            let slippage_k_bps = u32::try_from(lane_words[4]).map_err(|_| {
                SourceError::Unavailable("lane slippageKBps does not fit uint32".into())
            })?;
            state.lanes.insert(
                *asset,
                LaneState {
                    slot0: lane_words[0],
                    exists: decode_bool(lane_words[1])?,
                    paused: decode_bool(lane_words[2])?,
                    block_delay,
                    slippage_k_bps,
                },
            );
            state.total_principal_amount.insert(
                *asset,
                checked_u128(reserve_words[4], "totalPrincipalAmount")?,
            );
        }

        let mut partner_assets = assets.clone();
        if !partner_assets.contains(&cash) {
            partner_assets.push(cash);
        }
        for router in routers {
            let whitelist = decode_bool(decode_word(
                &self
                    .rpc
                    .call_at(
                        config.core,
                        selector_address(SELECTOR_WHITELIST, *router),
                        &self.snapshot_tag,
                    )
                    .await?,
                0,
            )?)?;
            state.whitelist.insert(*router, whitelist);
            for asset in &partner_assets {
                let fee = decode_word(
                    &self
                        .rpc
                        .call_at(
                            config.core,
                            selector_two_addresses(SELECTOR_PARTNERS, *router, *asset),
                            &self.snapshot_tag,
                        )
                        .await?,
                    1,
                )?;
                state.partner_fee_bps.insert(
                    (*router, *asset),
                    U256::from(checked_u32(fee, "partner fee")?),
                );
            }
        }
        Ok(BootstrapSnapshot {
            state,
            cursor,
            runtime_code_hash,
        })
    }
}

impl RpcSnapshotProvider {
    async fn resolve_lane_assets(
        &self,
        config: &DeploymentConfig,
        explicit: &[Address],
        snapshot_block: u64,
    ) -> Result<Vec<Address>, SourceError> {
        let mut history = Vec::new();
        for topic in lane_discovery_topics() {
            let request = BackfillRequest {
                from_block: config.deployment_block,
                to_block: snapshot_block,
                filter: crate::ContractFilter {
                    address: config.core,
                    topics: vec![topic],
                },
            };
            history.extend(
                self.rpc
                    .get_logs(&request, config.chain_id, Commitment::Canonical)
                    .await?,
            );
        }
        history.sort_by_key(|log| log.cursor.event_order());
        let mut discovered = BTreeSet::new();
        for log in history {
            let Some(topic0) = log.topics.first().copied() else {
                continue;
            };
            let Some(asset_word) = log.topics.get(1).copied() else {
                continue;
            };
            let asset = decode_address_word(&format!(
                "0x{}",
                hex_encode(&asset_word.to_be_bytes::<32>())
            ))?;
            if topic0 == TOPIC_LANE_ADDED {
                discovered.insert(asset);
            } else if topic0 == TOPIC_LANE_REMOVED {
                discovered.remove(&asset);
            }
        }
        if explicit.is_empty() {
            return Ok(discovered.into_iter().collect());
        }
        if explicit.iter().any(|asset| !discovered.contains(asset)) {
            return Err(SourceError::Unavailable(
                "explicit lane asset was not active in deployment history".into(),
            ));
        }
        Ok(explicit.to_vec())
    }
}

