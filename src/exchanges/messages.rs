use chrono::{DateTime, Utc};
use engineering_challenge::Side;

#[derive(Debug)]
pub enum Command {
    Unsubscribe { symbols: Vec<String> },
    Subscribe { symbols: Vec<String> },
}

#[derive(PartialEq, Debug)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

#[derive(PartialEq, Debug)]
pub enum MarketEvent {
    Snapshot {
        symbol: String,
        exchange_time: DateTime<Utc>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    LevelUpdate {
        exchange_time: DateTime<Utc>,
        symbol: String,
        side: Side,
        price: i64,
        new_quantity: i64,
    },
    SymbolAdded {
        symbol: String,
    },
    SymbolRemoved {
        symbol: String,
    },
}
