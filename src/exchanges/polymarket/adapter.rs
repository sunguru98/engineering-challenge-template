use std::{collections::HashSet, time::Duration};

use anyhow::Result;
use tokio_tungstenite::tungstenite::Message;

use crate::exchanges::{
    adapter::Adapter,
    messages::{Command, MarketEvent},
    polymarket::{
        constants::{BATCH_SIZE, HEARTBEAT_TIMER_SECONDS, WS_URL},
        decode::decode,
        discovery::fetch_all_markets,
        encode::{encode_command, encode_heartbeat_message, encode_initial_subscribe},
    },
};

#[derive(Clone, Copy)]
pub struct Polymarket {}

impl Adapter for Polymarket {
    async fn discover(&self) -> Result<Vec<String>> {
        fetch_all_markets().await
    }

    fn decode(&self, message: &str) -> Vec<MarketEvent> {
        decode(message)
    }

    fn encode_command(&self, command: &Command) -> String {
        encode_command(command)
    }

    fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(HEARTBEAT_TIMER_SECONDS)
    }

    fn heartbeat_message(&self) -> Message {
        encode_heartbeat_message()
    }

    fn initial_subscribe(&self, symbols: &HashSet<String>) -> String {
        encode_initial_subscribe(symbols)
    }

    fn symbols_per_socket(&self) -> usize {
        BATCH_SIZE
    }

    fn websocket_url(&self) -> String {
        WS_URL.to_string()
    }
}
