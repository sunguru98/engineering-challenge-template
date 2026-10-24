use anyhow::Context;
use engineering_challenge::{
    Side,
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
};
use futures_util::{SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};

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

#[derive(Serialize)]
pub struct SubscriptionPayload {
    pub assets_ids: Vec<&'static str>,
    pub r#type: &'static str,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PriceData {
    pub price: String,
    pub size: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BookEvent {
    pub market: String,
    pub asset_id: String,
    pub timestamp: String,
    pub bids: Vec<PriceData>,
    pub asks: Vec<PriceData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PriceChangeEvent {
    pub timestamp: String,
    pub price_changes: Vec<PriceChange>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PriceChange {
    pub asset_id: String,
    pub price: String,
    pub side: String,
    pub size: String,
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let url: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    let (stream, response) = tokio_tungstenite::connect_async(url).await?;

    println!("Connected: {}", response.status());
    let (mut write, mut read) = stream.split();

    let assets_ids =
        vec!["32338220190071351435772801779725302244575775216413325951443816017994629993401"];

    let payload = serde_json::to_string(&SubscriptionPayload {
        assets_ids: assets_ids.clone(),
        r#type: "market",
    })?;

    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            payload.into(),
        ))
        .await?;

    println!("Subscription done :)");

    while let Some(response_stream) = read.next().await {
        match response_stream {
            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                // std::fs::write("data.json", &text)?;
                let data = serde_json::from_str::<serde_json::Value>(&text)?;
                let events = match data {
                    serde_json::Value::Array(items) => items.into_iter().collect(),
                    serde_json::Value::Object(_) => vec![data],
                    _ => vec![],
                };

                for event in events {
                    if let Some(event_type) = event["event_type"].as_str() {
                        if event_type == "book" {
                            let book_event = serde_json::from_value::<BookEvent>(event)?;
                            let asks = book_event
                                .asks
                                .iter()
                                .map(|ask| {
                                    Ok(SnapshotAsksEntryDomain {
                                        price_level: convert_to_schema_int(&ask.price)?,
                                        quantity: convert_to_schema_int(&ask.size)?,
                                    })
                                })
                                .collect::<anyhow::Result<Vec<SnapshotAsksEntryDomain>>>()?;

                            let bids = book_event
                                .bids
                                .iter()
                                .map(|ask| {
                                    Ok(SnapshotBidsEntryDomain {
                                        price_level: convert_to_schema_int(&ask.price)?,
                                        quantity: convert_to_schema_int(&ask.size)?,
                                    })
                                })
                                .collect::<anyhow::Result<Vec<SnapshotBidsEntryDomain>>>()?;

                            let snapshot = SnapshotDomain {
                                asks,
                                bids,
                                exchange_timestamp: chrono::DateTime::from_timestamp_millis(
                                    book_event.timestamp.parse()?,
                                )
                                .context("Failed to parse timestamp")?,
                                received_at: chrono::Utc::now(),
                                symbol: book_event.asset_id,
                            };
                            // println!("snapshot: {snapshot:?}");
                        } else if event_type == "price_change" {
                            let price_change_event =
                                serde_json::from_value::<PriceChangeEvent>(event)?;
                            for pc in price_change_event.price_changes {
                                if assets_ids.iter().any(|&a| a == &pc.asset_id) {
                                    let price_level_update = PriceLevelUpdateDomain {
                                        exchange_timestamp:
                                            chrono::DateTime::from_timestamp_millis(
                                                price_change_event.timestamp.parse()?,
                                            )
                                            .context("Failed to parse timestamp")?,
                                        backtest_timestamp: chrono::Utc::now(),
                                        price_level: convert_to_schema_int(&pc.price)?,
                                        quantity: convert_to_schema_int(&pc.size)?,
                                        side: if &pc.side == "SELL" {
                                            Side::Sell
                                        } else {
                                            Side::Buy
                                        },
                                        symbol: pc.asset_id,
                                    };

                                    println!("{price_level_update:?}");
                                }
                            }
                        }
                    }
                }
            }
            Ok(msg) => {
                println!("Other message: {msg}")
            }
            Err(e) => eprintln!("{e:?}"),
        }
    }

    return Ok(());
}
