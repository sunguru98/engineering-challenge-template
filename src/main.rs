use std::collections::HashMap;

use anyhow::Context;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use serde::Deserialize;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;

const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

#[derive(serde::Serialize)]
pub struct SubscriptionPayload {
    pub assets_ids: Vec<String>,
    pub r#type: &'static str,
    pub custom_feature_enabled: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PriceData {
    pub price: String,
    pub size: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct BookEvent {
    pub asset_id: String,
    pub timestamp: String,
    pub bids: Vec<PriceData>,
    pub asks: Vec<PriceData>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PriceChangeEvent {
    pub timestamp: String,
    pub price_changes: Vec<PriceChange>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct MarketResolvedEvent {
    pub assets_ids: Vec<String>,
    pub timestamp: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PriceChange {
    pub asset_id: String,
    pub price: String,
    pub side: String,
    pub size: String,
}

#[derive(Debug)]
pub enum EType {
    Book,
    PriceChange,
    NewMarket,
    MarketResolved,
}

#[derive(Debug)]
pub struct Event {
    pub e_type: EType,
    pub data: serde_json::Value,
}

#[derive(Debug)]
pub enum UpdateEvents {
    MarketResolved { asset_ids: Vec<String> },
    NewMarket { asset_ids: Vec<String> },
}

#[derive(Debug)]
pub struct Orderbook {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub bids: std::collections::BTreeMap<i64, i64>,
    pub asks: std::collections::BTreeMap<i64, i64>,
}

pub fn extract_asset_id(market: &serde_json::Value) -> anyhow::Result<Option<String>> {
    if market["active"].as_bool() == Some(true) {
        let clob_token_ids = market["clob_token_ids"].as_array();
        if clob_token_ids.is_none() {
            return Ok(None);
        }

        let clob_token_ids = clob_token_ids.unwrap();
        if clob_token_ids.len() != 2 {
            return Ok(None);
        }

        if let Some(token_id) = clob_token_ids[0].as_str() {
            return Ok(Some(token_id.into()));
        }
    }

    Ok(None)
}

pub fn fetch_asset_ids() -> anyhow::Result<(
    HashMap<String, Orderbook>,
    Vec<Vec<String>>,
    HashMap<String, usize>,
)> {
    let token_ids = std::fs::read_to_string("token_ids.txt")?;
    let mut assets_ids = std::collections::HashMap::new();

    let mut batches: Vec<Vec<String>> = vec![];
    let mut batch: Vec<String> = vec![];
    let mut batch_counter = 1usize;

    let mut batch_idx = 0usize;
    let mut asset_batch_map = HashMap::new();

    for token_id in token_ids.lines().map(String::from).into_iter() {
        batch.push(token_id.clone());
        asset_batch_map.insert(token_id.clone(), batch_idx);
        if batch_counter == 5_000 {
            batches.push(batch);
            batch = vec![];
            batch_counter = 0;
            batch_idx += 1;
        } else {
            batch_counter += 1;
        }

        assets_ids.insert(
            token_id,
            Orderbook {
                asks: std::collections::BTreeMap::new(),
                bids: std::collections::BTreeMap::new(),
                timestamp: chrono::Utc::now(),
            },
        );
    }

    if !batch.is_empty() {
        batches.push(batch)
    }

    Ok((assets_ids, batches, asset_batch_map))
}

fn convert_to_schema_int(input: &str) -> anyhow::Result<i64> {
    let mut fractional = 0;
    let numerical;

    if input.contains("-") {
        anyhow::bail!("Invalid input: negative data");
    }

    if input.contains(".") {
        let (number, decimal) = input.split_once(".").unwrap();
        if decimal.len() > 6 || decimal.is_empty() {
            anyhow::bail!("Invalid input: invalid decimal")
        }

        if number.is_empty() {
            anyhow::bail!("Invalid input: invalid number");
        }

        let mut decimal_padded = decimal.to_owned();
        decimal_padded.push_str(&"0".repeat(6 - decimal.len()));
        fractional = decimal_padded.parse::<i64>()?;
        numerical = number.parse::<i64>()?;
    } else {
        numerical = input.parse::<i64>()?;
    }

    if numerical < 0 {
        anyhow::bail!("Invalid input: negative numbers");
    }

    numerical
        .checked_mul(1_000_000)
        .and_then(|n| n.checked_add(fractional))
        .context("Invalid input: numeric overflow")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = std::path::Path::new("data.bin");
    let mut writer = AlignedSbeWriter::create(file, CHUNK_SIZE_64_MEBIBYTES)?;

    let (mut assets_ids, batches, mut asset_worker_map) = fetch_asset_ids()?;
    println!("Total assets: {}", assets_ids.len());
    let batches_len = batches.len();
    println!("Total batches: {batches_len}");

    let mut batch_sender_map = HashMap::new();

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Event>(25_000);
    let (signal_sender, signal_receiver) = tokio::sync::watch::channel(false);

    let mut join_set: JoinSet<anyhow::Result<usize>> = tokio::task::JoinSet::new();
    for (batch_idx, batch) in batches.into_iter().enumerate() {
        let payload = serde_json::to_string(&SubscriptionPayload {
            assets_ids: batch,
            r#type: "market",
            custom_feature_enabled: true,
        })?;

        let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel::<UpdateEvents>(100);
        batch_sender_map.insert(batch_idx, event_sender);
        let mut event_sender_closed = false;

        let mut signal_receiver = signal_receiver.clone();
        let sender = sender.clone();

        join_set.spawn(async move {
            let mut attempt_number = 1;
            let mut delay_seconds = std::time::Duration::from_secs(1);
            let maximum_delay_seconds = std::time::Duration::from_secs(30);

            loop {
                let result: anyhow::Result<()> = async {
                    let (stream, _) = tokio_tungstenite::connect_async(
                        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
                    )
                    .await?;

                    let (mut write, mut read) = stream.split();

                    write
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            payload.clone().into(),
                        ))
                        .await?;

                    println!("Subscription done for websocket batch #{batch_idx} :)");

                    let mut heartbeat_interval =
                        tokio::time::interval(std::time::Duration::from_secs(10));
                    heartbeat_interval.tick().await;

                    loop {
                        tokio::select! {
                            _ = signal_receiver.changed() => {
                                if *signal_receiver.borrow() {
                                    break;
                                }
                            }
                            _ = heartbeat_interval.tick() => {
                                println!("Heartbeat check for #{batch_idx}");
                                write.send(tokio_tungstenite::tungstenite::Message::Text("PING".into())).await?;
                            }

                            update_event = event_receiver.recv(), if !event_sender_closed => {
                                match update_event {
                                    Some(update_event) => {
                                        match update_event {
                                            UpdateEvents::MarketResolved { asset_ids } => {
                                                write.send(format!(r#"
                                                {{
                                                    \"operation\": \"unsubscribe\",
                                                    \"assets_ids\": {}
                                                }}"#, serde_json::to_string(&asset_ids)?).into()).await?;
                                                println!("Unsubscribed market of {asset_ids:?} succesfully")
                                            },
                                            UpdateEvents::NewMarket { asset_ids } => {
                                                write.send(format!(r#"
                                                {{
                                                    \"operation\": \"subscribe\",
                                                    \"assets_ids\": {}
                                                }}"#, serde_json::to_string(&asset_ids)?).into()).await?;
                                                println!("New market of {asset_ids:?} added succesfully")
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
                                    Ok(other_msg) => println!("Other message received: {other_msg}"),
                                    Err(e) => anyhow::bail!(e),
                                }
                            }
                        }
                    }
                    Ok(())
                }
                .await;

                if let Err(err) = result {
                    eprintln!("Connection error: {err}")
                }

                if *signal_receiver.borrow() {
                    println!("Shutting down websocket batch #{batch_idx}");
                    break;
                }
                let jitter_ms = std::time::Duration::from_millis(rand::rng().random_range(0..750));
                let waiting_seconds = delay_seconds + jitter_ms;
                tokio::select! {
                    _ = signal_receiver.changed() => {
                        if *signal_receiver.borrow() {
                            println!("Shutting down websocket batch #{batch_idx}");
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
        });
    }
    drop(sender);

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.tick().await;

    let mut shutting_down = false;
    let mut all_events_processed = false;
    let mut work_complete = false;
    let mut next_batch_idx = 0;

    let sigint = tokio::signal::ctrl_c();
    tokio::pin!(sigint);
    loop {
        if shutting_down && all_events_processed && work_complete {
            break;
        }

        tokio::select! {
            _ = &mut sigint, if !shutting_down => {
                println!("\nCommencing Shutdown");
                signal_sender.send(true)?;
                shutting_down = true;
            }

            _ = interval.tick() => {
                println!("Creating a snapshot at {}", chrono::Utc::now());
                for (asset_id, orderbook) in assets_ids.iter() {
                    let mut asks = vec![];
                    for (price_level, quantity) in orderbook.asks.iter() {
                        asks.push(SnapshotAsksEntryDomain {
                            price_level: *price_level,
                            quantity: *quantity,
                        })
                    }

                    let mut bids = vec![];
                    for (price_level, quantity) in orderbook.bids.iter().rev() {
                        bids.push(SnapshotBidsEntryDomain {
                            price_level: *price_level,
                            quantity: *quantity,
                        })
                    }

                    let snapshot = SnapshotDomain {
                        asks,
                        bids,
                        exchange_timestamp: orderbook.timestamp,
                        received_at: chrono::Utc::now(),
                        symbol: asset_id.to_owned(),
                    };
                    // println!("{snapshot:?}");
                    writer.write_msg(&snapshot)?;
                }
            }

            join_result = join_set.join_next(), if shutting_down && !work_complete => {
                match join_result {
                    Some(Ok(Ok(batch_idx))) => println!("Websocket #{batch_idx} finished running"),
                    Some(Ok(Err(err))) => eprintln!("Websocket finished with error: {err:?}"),
                    Some(Err(err)) => eprintln!("Task returned with error: {err:?}"),
                    None => {
                        println!("All websockets closed");
                        work_complete = true
                    }
                }
            }

            event = receiver.recv(), if !all_events_processed => {
                match event {
                    None => {
                        all_events_processed = true;
                    }
                    Some(event) => {
                        let e_data = event.data;
                        match event.e_type {
                            EType::MarketResolved => {
                                let market_resolved_event = MarketResolvedEvent::deserialize(&e_data)?;
                                let mut unsubscribing_ids = vec![];
                                for asset_id in market_resolved_event.assets_ids.into_iter() {
                                    if assets_ids.get(&asset_id).is_some() {
                                        unsubscribing_ids.push(asset_id);
                                    }
                                }

                                for id in unsubscribing_ids.into_iter() {
                                    if let Some(batch_idx) = asset_worker_map.get(&id) {
                                        if let Some(sender) = batch_sender_map.get(&batch_idx) {
                                            println!("Marking {id} as resolved to batch #{batch_idx}");
                                            sender.send(
                                                UpdateEvents::MarketResolved {
                                                    asset_ids: vec![id.clone()]
                                                }
                                            ).await?;
                                            assets_ids.remove(&id);
                                            asset_worker_map.remove(&id);
                                        }
                                    }
                                }
                            },
                            EType::NewMarket => {
                                if let Some(asset_id) = extract_asset_id(&e_data)? {
                                    println!("New market notified of asset_id: {asset_id}");
                                    if assets_ids.get(&asset_id).is_none() {
                                        if let Some(sender) = batch_sender_map.get(&next_batch_idx) {
                                            sender.send(
                                                UpdateEvents::NewMarket{
                                                    asset_ids: vec![asset_id.clone()]
                                                }
                                            ).await?;

                                            assets_ids.insert(
                                                asset_id.clone(),
                                                Orderbook {
                                                    timestamp: chrono::Utc::now(),
                                                    bids: std::collections::BTreeMap::new(),
                                                    asks: std::collections::BTreeMap::new(),
                                                }
                                            );
                                            asset_worker_map.insert(asset_id.clone(), next_batch_idx);
                                            next_batch_idx = (next_batch_idx + 1) % batches_len;
                                        };
                                    }
                                }
                            },
                            EType::Book => {
                                let asset_id = e_data["asset_id"].as_str().context("Asset id not found")?;
                                let Some(orderbook) = assets_ids.get_mut(asset_id) else {
                                    continue;
                                };

                                let timestamp = e_data["timestamp"]
                                    .as_str()
                                    .context("Timestamp not found")?;

                                orderbook.timestamp = chrono::DateTime::from_timestamp_millis(timestamp.parse()?)
                                    .context("Failed to parse timestamp")?;
                                orderbook.asks.clear();
                                orderbook.bids.clear();

                                let book_event = BookEvent::deserialize(&e_data)?;
                                for ask in book_event.asks {
                                    let ask_price = convert_to_schema_int(&ask.price)?;
                                    let ask_size = convert_to_schema_int(&ask.size)?;
                                    orderbook.asks.insert(ask_price, ask_size);
                                }

                                for bid in book_event.bids {
                                    let bid_price = convert_to_schema_int(&bid.price)?;
                                    let bid_size = convert_to_schema_int(&bid.size)?;
                                    orderbook.bids.insert(bid_price, bid_size);
                                }
                            }
                            EType::PriceChange => {
                                let mut price_change_event = PriceChangeEvent::deserialize(&e_data)?;
                                for pc in price_change_event.price_changes.iter_mut() {
                                    if let Some(orderbook) = assets_ids.get_mut(&pc.asset_id) {
                                        let price_level = convert_to_schema_int(&pc.price)?;
                                        let quantity = convert_to_schema_int(&pc.size)?;
                                        let side = if &pc.side == "SELL" {
                                            engineering_challenge::Side::Sell
                                        } else if &pc.side == "BUY" {
                                            engineering_challenge::Side::Buy
                                        } else {
                                            anyhow::bail!("Invalid side");
                                        };

                                        let exchange_timestamp = chrono::DateTime::from_timestamp_millis(
                                            price_change_event.timestamp.parse()?,
                                        )
                                        .context("Failed to parse timestamp")?;

                                        orderbook.timestamp = exchange_timestamp;
                                        let price_level_update = PriceLevelUpdateDomain {
                                            exchange_timestamp,
                                            backtest_timestamp: chrono::Utc::now(),
                                            price_level,
                                            quantity,
                                            side,
                                            symbol: std::mem::take(&mut pc.asset_id),
                                        };


                                        if quantity == 0 {
                                            match side {
                                                engineering_challenge::Side::Buy => orderbook.bids.remove(&price_level),
                                                engineering_challenge::Side::Sell => orderbook.asks.remove(&price_level),
                                            };
                                        } else {
                                            match side {
                                                engineering_challenge::Side::Buy => orderbook.bids.insert(price_level, quantity),
                                                engineering_challenge::Side::Sell => orderbook.asks.insert(price_level, quantity),
                                            };
                                        }

                                        // println!("{price_level_update:?}");
                                        writer.write_msg(&price_level_update)?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    writer.flush()?;
    Ok(())
}
