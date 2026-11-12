use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, TimeDelta, Utc};
use engineering_challenge::Side;

use crate::{constants::STALL_TIMER_SECONDS, messages::Level};

#[derive(Debug, PartialEq)]
pub enum WriteAction {
    Snapshot,
    Update,
    Nothing,
}

#[derive(Debug)]
pub struct Orderbook {
    pub timestamp: DateTime<Utc>,
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
}

impl Orderbook {
    pub fn new() -> Self {
        Self {
            timestamp: Utc::now(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct Books {
    pub socket_orderbook: HashMap<usize, Orderbook>,
    pub primary_socket_id: Option<usize>,
    pub stalled_since: Option<DateTime<Utc>>,
}

impl Books {
    pub fn new() -> Self {
        Self {
            socket_orderbook: HashMap::new(),
            primary_socket_id: None,
            stalled_since: None,
        }
    }

    pub fn primary_book(&self) -> Option<&Orderbook> {
        let Some(socket_id) = self.primary_socket_id else {
            return None;
        };

        self.socket_orderbook.get(&socket_id)
    }

    pub fn drop_socket(&mut self, socket_id: usize) -> WriteAction {
        self.stalled_since = None;
        let is_primary_socket = self.primary_socket_id == Some(socket_id);
        self.socket_orderbook.remove(&socket_id);
        if !is_primary_socket {
            return WriteAction::Nothing;
        }

        match self.socket_orderbook.keys().next() {
            Some(other_socket_id) => {
                self.primary_socket_id = Some(*other_socket_id);
                return WriteAction::Snapshot;
            }
            None => {
                self.primary_socket_id = None;
                return WriteAction::Nothing;
            }
        };
    }

    pub fn apply_snapshot(
        &mut self,
        socket_id: usize,
        timestamp: DateTime<Utc>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    ) {
        if self.primary_socket_id.is_none() {
            self.primary_socket_id = Some(socket_id);
        }

        let asks: BTreeMap<i64, i64> = asks
            .into_iter()
            .map(|level| (level.price, level.quantity))
            .collect();

        let bids: BTreeMap<i64, i64> = bids
            .into_iter()
            .map(|level| (level.price, level.quantity))
            .collect();

        let mut orderbook = Orderbook::new();
        orderbook.timestamp = timestamp;
        orderbook.asks = asks;
        orderbook.bids = bids;

        self.socket_orderbook.insert(socket_id, orderbook);
    }

    pub fn apply_update(
        &mut self,
        socket_id: usize,
        timestamp: DateTime<Utc>,
        side: Side,
        price: i64,
        quantity: i64,
        received_at: DateTime<Utc>,
    ) -> WriteAction {
        if self.primary_socket_id.is_none() {
            self.primary_socket_id = Some(socket_id);
        }

        let orderbook = self
            .socket_orderbook
            .entry(socket_id)
            .or_insert_with(|| Orderbook::new());

        orderbook.timestamp = timestamp;

        let book_side = match side {
            Side::Buy => &mut orderbook.bids,
            Side::Sell => &mut orderbook.asks,
        };
        if quantity == 0 {
            book_side.remove(&price);
        } else {
            book_side.insert(price, quantity);
        }

        if self.primary_socket_id == Some(socket_id) {
            self.stalled_since = None;
            return WriteAction::Update;
        }

        match self.stalled_since {
            None => {
                self.stalled_since = Some(received_at);
                return WriteAction::Nothing;
            }
            Some(t) => {
                let time_elapsed = received_at - t;
                if time_elapsed >= TimeDelta::seconds(STALL_TIMER_SECONDS) {
                    self.primary_socket_id = Some(socket_id);
                    self.stalled_since = None;
                    return WriteAction::Snapshot;
                }
                return WriteAction::Nothing;
            }
        }
    }
}
