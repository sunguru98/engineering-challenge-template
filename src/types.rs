use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Sender;

use crate::messages::{Command, MarketEvent};
use crate::orderbook::Books;

pub type SymbolBooksMap = HashMap<String, Books>;
pub type Batches = Vec<Vec<String>>;
pub type SymbolBatchMap = HashMap<String, usize>;
pub type SocketEventSenderMap = HashMap<usize, Sender<Command>>;
pub type BatchSocketIdsMap = HashMap<usize, HashSet<usize>>;

#[derive(serde::Serialize)]
pub struct SubscriptionPayload {
    pub assets_ids: Vec<String>,
    pub r#type: &'static str,
    pub custom_feature_enabled: bool,
    pub initial_dump: bool,
}

pub struct IncomingMessage {
    pub socket_id: usize,
    pub received_at: DateTime<Utc>,
    pub market_events: Vec<MarketEvent>,
}
