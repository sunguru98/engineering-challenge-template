use anyhow::Context;
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;

const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

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

#[derive(serde::Serialize)]
pub struct SubscriptionPayload<'a> {
    pub assets_ids: Vec<&'a str>,
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
    pub bids: std::collections::BTreeMap<i64, i64>,
    pub asks: std::collections::BTreeMap<i64, i64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = std::path::Path::new("data.bin");
    let mut writer = AlignedSbeWriter::create(file, CHUNK_SIZE_64_MEBIBYTES)?;
    // writer.write_msg(&snapshot)?;

    let url: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    let (stream, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut write, mut read) = stream.split();

    let mut assets_ids = std::collections::HashMap::new();
    assets_ids.insert(
        "32338220190071351435772801779725302244575775216413325951443816017994629993401".to_string(),
        Orderbook {
            asks: std::collections::BTreeMap::new(),
            bids: std::collections::BTreeMap::new(),
        },
    );

    let payload = serde_json::to_string(&SubscriptionPayload {
        assets_ids: assets_ids.keys().map(String::as_str).collect(),
        r#type: "market",
    })?;

    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            payload.into(),
        ))
        .await?;

    println!("Subscription done :)");

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
                        exchange_timestamp: chrono::Utc::now(),
                        received_at: chrono::Utc::now(),
                        symbol: asset_id.to_owned(),
                    };

                    writer.write_msg(&snapshot)?;
                }
            }

            response_stream = read.next() => {
                match response_stream {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                            // std::fs::write("data.json", &text)?;
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

                            if event_type == "book" {
                                let asset_id = event["asset_id"].as_str().context("Asset id not found")?;
                                let orderbook = assets_ids.get_mut(asset_id).context("Invalid asset id")?;

                                orderbook.asks.clear();
                                orderbook.bids.clear();

                                let book_event = BookEvent::deserialize(&event)?;
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
                            } else if event_type == "price_change" {
                                let mut price_change_event = PriceChangeEvent::deserialize(&event)?;
                                for pc in price_change_event.price_changes.iter_mut() {
                                    if let Some(orderbook) = assets_ids.get_mut(&pc.asset_id) {
                                        let price_level = convert_to_schema_int(&pc.price)?;
                                        let quantity = convert_to_schema_int(&pc.size)?;
                                        let side = if &pc.side == "SELL" {
                                            Side::Sell
                                        } else if &pc.side == "BUY" {
                                            Side::Buy
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
                                                Side::Buy => orderbook.bids.remove(&price_level),
                                                Side::Sell => orderbook.asks.remove(&price_level),
                                            };
                                        } else {
                                            match side {
                                                Side::Buy => orderbook.bids.insert(price_level, quantity),
                                                Side::Sell => orderbook.asks.insert(price_level, quantity),
                                            };
                                        }

                                        writer.write_msg(&price_level_update)?;
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(msg)) => {
                        println!("Other message: {msg}");
                    }
                    Some(Err(e)) => {
                        eprintln!("{e:?}");
                        break;
                    }
                    None => {
                        println!("Connection closed");
                        break;
                    }
                }
            }
        }
    }

    writer.flush()?;
    Ok(())
}

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
