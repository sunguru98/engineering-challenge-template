use std::collections::HashSet;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use tokio::{
    sync::{mpsc, watch},
    time::interval,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    constants::{HEARTBEAT_TIMER_SECONDS, WS_URL},
    types::{EType, Event, SubscriptionPayload, UpdateEvents},
};

pub async fn run_worker(
    batch_idx: usize,
    batch: Vec<String>,
    sender: mpsc::Sender<Event>,
    mut event_receiver: mpsc::Receiver<UpdateEvents>,
    mut signal_receiver: watch::Receiver<bool>,
) -> anyhow::Result<usize> {
    let mut event_sender_closed = false;

    let mut attempt_number = 1;
    let mut delay_seconds = std::time::Duration::from_secs(1);
    let maximum_delay_seconds = std::time::Duration::from_secs(30);

    let mut worker_payload: HashSet<String> = HashSet::new();
    for key in batch.into_iter() {
        worker_payload.insert(key);
    }

    let mut heartbeat_interval = interval(Duration::from_secs(HEARTBEAT_TIMER_SECONDS));
    heartbeat_interval.tick().await;

    loop {
        let connected_at = std::time::Instant::now();

        let result: anyhow::Result<()> = async {
            let (stream, _) = connect_async(
                WS_URL,
            )
            .await?;

            let (mut write, mut read) = stream.split();

            let payload = serde_json::to_string(&SubscriptionPayload {
                assets_ids: worker_payload.iter().map(String::from).collect(),
                r#type: "market",
                custom_feature_enabled: true,
                initial_dump: true,
            })?;

            write
                .send(Message::Text(
                    payload.into(),
                ))
                .await?;

            eprintln!("Subscription done for websocket batch #{batch_idx} :)");


            loop {
                tokio::select! {
                    _ = signal_receiver.changed() => {
                        if *signal_receiver.borrow() {
                            break;
                        }
                    }
                    _ = heartbeat_interval.tick() => {
                        write.send(Message::Text("PING".into())).await?;
                    }

                    update_event = event_receiver.recv(), if !event_sender_closed => {
                        match update_event {
                            Some(update_event) => {
                                match update_event {
                                    UpdateEvents::MarketResolved { asset_ids } => {
                                        let payload = serde_json::json!({
                                            "operation": "unsubscribe",
                                            "assets_ids": asset_ids,
                                        });
                                        write.send(payload.to_string().into()).await?;
                                        eprintln!("Unsubscribed market of {asset_ids:?} succesfully");
                                        for id in asset_ids {
                                            worker_payload.remove(&id);
                                        }
                                    },
                                    UpdateEvents::NewMarket { asset_ids } => {
                                        let payload = serde_json::json!({
                                            "operation": "subscribe",
                                            "assets_ids": asset_ids,
                                        });
                                        write.send(payload.to_string().into()).await?;
                                        eprintln!("New market of {asset_ids:?} added succesfully");
                                        for id in asset_ids {
                                            worker_payload.insert(id);
                                        }
                                    }
                                }
                            },
                            None => {
                                event_sender_closed = true
                            }
                        }
                    }

                    response = read.next() => {
                        let Some(response) = response else {
                            anyhow::bail!("Stream ended unxpectedly");
                        };

                        match response {
                            Ok(Message::Text(text)) => {
                                if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text){
                                    let events = match data {
                                        serde_json::Value::Array(items) => items.into_iter().collect(),
                                        serde_json::Value::Object(_) => vec![data],
                                        _ => vec![],
                                    };

                                    for event in events {
                                        let event_type = event["event_type"]
                                            .as_str()
                                            .context("Unable to find event_type")?;
                                        let event_type = match event_type {
                                            "book"  => EType::Book,
                                            "price_change" => EType::PriceChange,
                                            "new_market" => EType::NewMarket,
                                            "market_resolved" => EType::MarketResolved,
                                            _ => {
                                                continue
                                            }
                                        };

                                        let block_event = Event {
                                            data: event,
                                            e_type: event_type,
                                        };

                                        sender.send(block_event).await?;
                                    }
                                }
                            }
                            Ok(other_msg) => eprintln!("Other message received: {other_msg}"),
                            Err(e) => anyhow::bail!(e),
                        }
                    }
                }
            }
            Ok(())
        }.await;

        match result {
            Ok(()) => break,
            Err(err) => eprintln!("Connection error: {err}"),
        }

        if connected_at.elapsed() > Duration::from_secs(60) {
            attempt_number = 0;
            delay_seconds = Duration::from_secs(1);
        }

        let jitter_ms = Duration::from_millis(rand::rng().random_range(0..750));
        let waiting_seconds = delay_seconds + jitter_ms;
        tokio::select! {
            _ = signal_receiver.changed() => {
                if *signal_receiver.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(waiting_seconds) => {}
        }

        delay_seconds = (delay_seconds * 2).min(maximum_delay_seconds);
        attempt_number += 1;

        if attempt_number > 20 {
            anyhow::bail!("Connection failed");
        }
    }

    Ok(batch_idx)
}
