//! Convenient imports for applications embedding the network-independent client.
//!
//! Internal SDK crates use canonical module paths instead of this façade.

pub use crate::bootstrap::BootstrapSnapshot;
pub use crate::indexer::client::ConnectedQuoteClient;
pub use crate::indexer::client_types::{ClientConnectConfig, ClientRuntimeStatsSnapshot};
pub use crate::indexer::engine::QuoteIndexer;
pub use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
pub use crate::indexer::quote_types::{ClientBatchQuote, ClientQuote, IndexerHealth};
pub use crate::model::{
    BackfillRequest, ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractFilter, ContractLog,
    DeploymentConfig, LogDecodeError, Network, QuoteEvent, SourceError,
};
pub use crate::source::{ChainDataSource, SourceStream};
pub use crate::state::reducer::{QuoteReducer, ReducerError};
