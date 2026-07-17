//! Live Monad parser/indexer/RPC soak validation.

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use lunarbase_math::U256;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::watch;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};

mod comparison;
mod helpers;
mod parser_monitor;
mod runner;
mod types;

pub use runner::run;
pub use types::{MonadArguments, MonadError};
