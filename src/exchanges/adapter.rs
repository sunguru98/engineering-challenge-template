use std::{collections::HashSet, time::Duration};

use anyhow::Result;
use tokio_tungstenite::tungstenite::Message;

use crate::exchanges::messages::{Command, MarketEvent};

pub trait Adapter: Clone + Send + Sync + 'static {
    fn websocket_url(&self) -> String;
    fn symbols_per_socket(&self) -> usize;
    async fn discover(&self) -> Result<Vec<String>>;
    fn initial_subscribe(&self, symbols: &HashSet<String>) -> String;
    fn encode_command(&self, command: &Command) -> String;
    fn heartbeat_interval(&self) -> Duration;
    fn heartbeat_message(&self) -> Message;
    fn decode(&self, message: &str) -> Vec<MarketEvent>;
}
