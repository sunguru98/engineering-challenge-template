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
pub struct PriceChange {
    pub asset_id: String,
    pub price: String,
    pub side: String,
    pub size: String,
}

pub struct Orderbook {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub bids: std::collections::BTreeMap<i64, i64>,
    pub asks: std::collections::BTreeMap<i64, i64>,
}

pub fn fetch_asset_ids() -> anyhow::Result<(HashMap<String, Orderbook>, Vec<Vec<String>>)> {
    let token_ids = std::fs::read_to_string("token_ids.txt")?;
    let mut assets_ids = std::collections::HashMap::new();

    let mut batches: Vec<Vec<String>> = vec![];
    let mut batch: Vec<String> = vec![];
    let mut batch_counter = 1usize;

    for token_id in token_ids.lines().map(String::from).into_iter() {
        if batch_counter == 10_000 {
            batches.push(batch);
            batch = vec![];
            batch_counter = 0;
        } else {
            batch.push(token_id.clone());
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

    Ok((assets_ids, batches))
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

#[derive(Debug)]
pub enum EType {
    Book,
    PriceChange,
}

#[derive(Debug)]
pub struct Event {
    pub e_type: EType,
    pub data: serde_json::Value,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = std::path::Path::new("data.bin");
    let mut writer = AlignedSbeWriter::create(file, CHUNK_SIZE_64_MEBIBYTES)?;

    let (mut assets_ids, batches) = fetch_asset_ids()?;
    println!("Total assets: {}", assets_ids.len());
    println!("Total batches: {}", batches.len());

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Event>(1_000);

    let mut join_set: JoinSet<anyhow::Result<(), anyhow::Error>> = tokio::task::JoinSet::new();
    for (batch_idx, batch) in batches.into_iter().enumerate() {
        let sender = sender.clone();
        let payload = serde_json::to_string(&SubscriptionPayload {
            assets_ids: batch,
            r#type: "market",
        })?;

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

                    println!("Subscription done for batch#{batch_idx} :)");

                    while let Some(response) = read.next().await {
                        match response {
                            Ok(Message::Text(text)) => {
                                let data = serde_json::from_str::<serde_json::Value>(&text)?;
                                let events = match data {
                                    serde_json::Value::Array(items) => items.into_iter().collect(),
                                    serde_json::Value::Object(_) => vec![data],
                                    _ => vec![],
                                };

                                for event in events {
                                    let event_type = event["event_type"]
                                        .as_str()
                                        .context("Unable to find event_type")?;

                                    let event_type = if event_type == "book" {
                                        EType::Book
                                    } else if event_type == "price_change" {
                                        EType::PriceChange
                                    } else {
                                        anyhow::bail!("Invalid event received");
                                    };

                                    let block_event = Event {
                                        data: event,
                                        e_type: event_type,
                                    };

                                    sender.send(block_event).await?;
                                }
                            }
                            Ok(other_msg) => println!("Other message received: {other_msg}"),
                            Err(e) => anyhow::bail!(e),
                        }
                    }

                    anyhow::bail!("Stream ended unxpectedly");
                }
                .await;

                if let Err(err) = result {
                    eprintln!("Connection error: {err}")
                }

                let jitter_ms = std::time::Duration::from_millis(rand::rng().random_range(0..750));
                let waiting_seconds = delay_seconds + jitter_ms;
                tokio::time::sleep(waiting_seconds).await;

                delay_seconds = (delay_seconds * 2).min(maximum_delay_seconds);
                attempt_number += 1;

                if attempt_number > 20 {
                    anyhow::bail!("Connection failed");
                }
            }
        });
    }
    drop(sender);

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    interval.tick().await;

    loop {
        tokio::select! {
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

                    writer.write_msg(&snapshot)?;
                }
            }
            Some(event) = receiver.recv() => {
                let e_data = event.data;
                match event.e_type {
                    EType::Book => {
                        let asset_id = e_data["asset_id"].as_str().context("Asset id not found")?;
                        let orderbook = assets_ids.get_mut(asset_id).context("Invalid asset id")?;
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

                                let price_level_update = PriceLevelUpdateDomain {
                                    exchange_timestamp: chrono::DateTime::from_timestamp_millis(
                                        price_change_event.timestamp.parse()?,
                                    )
                                    .context("Failed to parse timestamp")?,
                                    backtest_timestamp: chrono::Utc::now(),
                                    price_level,
                                    quantity,
                                    side,
                                    symbol: std::mem::take(&mut pc.asset_id),
                                };

                                println!("{price_level_update:?}");

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

                                writer.write_msg(&price_level_update)?;
                            }
                        }
                    }
                }
            }
        }
    }
}

// loop {
//

//         response_stream = read.next() => {
//             match response_stream {
//                 Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
//                         // std::fs::write("data.json", &text)?;
//
//                 }
//                 Some(Ok(msg)) => {
//                     println!("Other message: {msg}");
//                 }
//                 Some(Err(e)) => {
//                     eprintln!("{e:?}");
//                     break;
//                 }
//                 None => {
//                     println!("Connection closed");
//                     break;
//                 }
//             }
//         }
//     }
// }

// writer.flush()?;

// let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file)?;

// let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg()? else {
//     anyhow::bail!("expected snapshot");
// };
// assert_eq!(SnapshotDomain::try_from_decoder(decoded)?, snapshot);

// let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg()? else {
//     anyhow::bail!("expected price level update");
// };
// assert_eq!(PriceLevelUpdateDomain::try_from_decoder(decoded)?, update);
// assert!(reader.read_msg()?.is_none());

// println!("Verification success!");
