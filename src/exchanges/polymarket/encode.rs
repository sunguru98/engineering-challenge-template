use tokio_tungstenite::tungstenite::Message;

use crate::exchanges::messages::Command;
use std::collections::HashSet;

pub fn encode_initial_subscribe(symbols: &HashSet<String>) -> String {
    let symbols: Vec<String> = symbols.iter().map(String::from).collect();

    let payload = serde_json::json!({
        "assets_ids": symbols,
        "type": "market",
        "custom_feature_enabled": true,
        "initial_dump": true,
    });

    payload.to_string()
}

pub fn encode_command(command: &Command) -> String {
    let payload = match command {
        Command::Unsubscribe { symbols } => {
            serde_json::json!({
                "operation": "unsubscribe",
                "assets_ids": symbols,
                "custom_feature_enabled": true,
            })
        }
        Command::Subscribe { symbols } => {
            serde_json::json!({
                "operation": "subscribe",
                "assets_ids": symbols,
                "custom_feature_enabled": true,
            })
        }
    };

    payload.to_string()
}

pub fn encode_heartbeat_message() -> Message {
    Message::Text("PING".into())
}
