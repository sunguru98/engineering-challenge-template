use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use crate::messages::{Level, MarketEvent};
use crate::parse::convert_to_schema_int;
use engineering_challenge::Side;

#[derive(Deserialize, Debug)]
pub struct PriceData {
    pub price: String,
    pub size: String,
}

#[derive(Deserialize, Debug)]
pub struct BookEvent {
    pub asset_id: String,
    pub timestamp: String,
    pub bids: Vec<PriceData>,
    pub asks: Vec<PriceData>,
}

#[derive(Deserialize, Debug)]
pub struct PriceChangeEvent {
    pub timestamp: String,
    pub price_changes: Vec<PriceChange>,
}

#[derive(Deserialize, Debug)]
pub struct MarketResolvedEvent {
    pub assets_ids: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct PriceChange {
    pub asset_id: String,
    pub price: String,
    pub side: String,
    pub size: String,
}

fn extract_asset_id(market: &Value) -> Option<String> {
    if market["active"].as_bool() != Some(true) {
        return None;
    }

    let ids = match market["clob_token_ids"].as_array() {
        Some(ids) => ids,
        None => {
            let Some(symbols) = market["assets_ids"].as_array() else {
                return None;
            };
            symbols
        }
    };

    if ids.len() != 2 {
        return None;
    }

    ids[0].as_str().map(String::from)
}

fn parse_side(side: &str) -> Option<Side> {
    match side {
        "BUY" => Some(Side::Buy),
        "SELL" => Some(Side::Sell),
        _ => None,
    }
}

fn parse_timestamp(raw_timestamp: String) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(raw_timestamp.parse()?).context("Failed to parse timestamp")
}

fn parse_levels(price_data: Vec<PriceData>) -> Result<Vec<Level>> {
    price_data
        .into_iter()
        .map(|pd| {
            let level = Level {
                price: convert_to_schema_int(&pd.price)?,
                quantity: convert_to_schema_int(&pd.size)?,
            };

            Ok(level)
        })
        .collect()
}

fn decode_book(event: &Value) -> Result<Vec<MarketEvent>> {
    let BookEvent {
        asset_id,
        timestamp,
        bids,
        asks,
    } = BookEvent::deserialize(event)?;
    let timestamp = parse_timestamp(timestamp)?;
    let asks = parse_levels(asks)?;
    let bids = parse_levels(bids)?;

    Ok(vec![MarketEvent::Snapshot {
        symbol: asset_id,
        exchange_time: timestamp,
        bids,
        asks,
    }])
}

fn decode_price_change(event: &Value) -> Result<Vec<MarketEvent>> {
    let PriceChangeEvent {
        timestamp,
        price_changes,
    } = PriceChangeEvent::deserialize(event)?;

    let timestamp = parse_timestamp(timestamp)?;

    let mut market_events = vec![];
    for pc in price_changes {
        let Some(side) = parse_side(&pc.side) else {
            continue;
        };

        let Ok(price) = convert_to_schema_int(&pc.price) else {
            eprintln!("Invalid price: {}", pc.price);
            continue;
        };

        let Ok(quantity) = convert_to_schema_int(&pc.size) else {
            eprintln!("Invalid size: {}", pc.size);
            continue;
        };

        market_events.push(MarketEvent::LevelUpdate {
            exchange_time: timestamp,
            symbol: pc.asset_id,
            side,
            price,
            new_quantity: quantity,
        })
    }

    Ok(market_events)
}

fn decode_new_market(event: &Value) -> Result<Vec<MarketEvent>> {
    let Some(symbol) = extract_asset_id(event) else {
        return Ok(vec![]);
    };

    Ok(vec![MarketEvent::SymbolAdded { symbol }])
}

fn decode_market_resolved(event: &Value) -> Result<Vec<MarketEvent>> {
    let MarketResolvedEvent { assets_ids, .. } = MarketResolvedEvent::deserialize(event)?;

    Ok(assets_ids
        .into_iter()
        .map(|symbol| MarketEvent::SymbolRemoved { symbol })
        .collect())
}

pub fn decode(raw_message: &str) -> Vec<MarketEvent> {
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw_message) else {
        return vec![];
    };

    let events = match json {
        serde_json::Value::Array(items) => items,
        serde_json::Value::Object(_) => vec![json],
        _ => vec![],
    };

    let mut all_events = Vec::<MarketEvent>::new();

    for event in events {
        let Some(event_type) = event["event_type"].as_str() else {
            continue;
        };

        if let Ok(mut market_events) = match event_type {
            "book" => decode_book(&event),
            "price_change" => decode_price_change(&event),
            "new_market" => decode_new_market(&event),
            "market_resolved" => decode_market_resolved(&event),
            _ => {
                continue;
            }
        } {
            all_events.append(&mut market_events)
        } else {
            eprintln!("Failed to decode {event_type}");
        }
    }

    all_events
}
