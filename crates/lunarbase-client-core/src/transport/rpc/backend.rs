#[derive(Clone)]
pub struct RpcHttpBackend {
    rpc: RpcHttpClient,
    network: Network,
    chain_id: u64,
    snapshot_tag: Arc<str>,
}

impl RpcHttpBackend {
    /// Creates the HTTP-only backend used for canonical snapshots/backfills.
    pub fn new(
        rpc: RpcHttpClient,
        network: Network,
        chain_id: u64,
        snapshot_tag: impl Into<String>,
    ) -> Self {
        Self {
            rpc,
            network,
            chain_id,
            snapshot_tag: Arc::from(snapshot_tag.into()),
        }
    }

    /// Returns the underlying JSON-RPC client.
    pub fn rpc(&self) -> &RpcHttpClient {
        &self.rpc
    }

    /// Returns the network family configured for this backend.
    pub fn network(&self) -> Network {
        self.network
    }

    /// Returns the configured chain id.
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }
}

#[async_trait]
impl NormalizedBackend for RpcHttpBackend {
    async fn snapshot_cursor(&self, network: Network) -> Result<ChainCursor, SourceError> {
        if network != self.network {
            return Err(SourceError::NetworkMismatch);
        }
        let commitment = if self.snapshot_tag.as_ref() == "finalized" {
            Commitment::Finalized
        } else {
            Commitment::Canonical
        };
        self.rpc
            .block_cursor(&self.snapshot_tag, self.chain_id, commitment)
            .await
            .map_err(Into::into)
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.rpc
            .get_logs(&request, self.chain_id, Commitment::Canonical)
            .await
            .map_err(Into::into)
    }

    async fn subscribe(
        &self,
        _network: Network,
        _filter: crate::ContractFilter,
    ) -> Result<SourceStream, SourceError> {
        Err(SourceError::Unavailable(
            "HTTP RPC backend has no realtime subscription; use a network source or WebSocket backend".into(),
        ))
    }
}

