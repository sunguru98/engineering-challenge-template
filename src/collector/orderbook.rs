use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, TimeDelta, Utc};
use engineering_challenge::Side;

use crate::{constants::STALL_TIMER_SECONDS, exchanges::messages::Level};

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
pub struct StallTimer {
    since: DateTime<Utc>,
    newest_exchange_time: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Books {
    socket_orderbook: HashMap<usize, Orderbook>,
    primary_socket_id: Option<usize>,
    stall_timer: Option<StallTimer>,
    create_from_first_update: bool,
}

impl Books {
    pub fn new() -> Self {
        Self {
            socket_orderbook: HashMap::new(),
            primary_socket_id: None,
            stall_timer: None,
            create_from_first_update: true,
        }
    }

    pub fn primary_book(&self) -> Option<&Orderbook> {
        let Some(socket_id) = self.primary_socket_id else {
            return None;
        };

        self.socket_orderbook.get(&socket_id)
    }

    pub fn drop_socket(&mut self, socket_id: usize) -> WriteAction {
        if self.socket_orderbook.remove(&socket_id).is_none() {
            return WriteAction::Nothing;
        };

        self.stall_timer = None;
        self.create_from_first_update = false;

        if self.primary_socket_id != Some(socket_id) {
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
    ) -> WriteAction {
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

        let changed = if let Some(old) = self.socket_orderbook.get(&socket_id) {
            old.bids != bids || old.asks != asks
        } else {
            !self.create_from_first_update
        };

        let mut orderbook = Orderbook::new();
        orderbook.timestamp = timestamp;
        orderbook.asks = asks;
        orderbook.bids = bids;

        self.socket_orderbook.insert(socket_id, orderbook);

        if self.primary_socket_id == Some(socket_id) {
            if let Some(stall_timer) = &self.stall_timer
                && timestamp >= stall_timer.newest_exchange_time
            {
                self.stall_timer = None;
            }

            if changed {
                return WriteAction::Snapshot;
            }
        }

        WriteAction::Nothing
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
        if self.socket_orderbook.get(&socket_id).is_none() {
            if !self.create_from_first_update {
                return WriteAction::Nothing;
            }
        }

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
            if let Some(stall_timer) = &self.stall_timer
                && timestamp >= stall_timer.newest_exchange_time
            {
                self.stall_timer = None;
            }

            return WriteAction::Update;
        }

        if let Some(primary_book) = self.primary_book() {
            if primary_book.timestamp >= timestamp {
                return WriteAction::Nothing;
            }

            match &mut self.stall_timer {
                None => {
                    self.stall_timer = Some(StallTimer {
                        since: received_at,
                        newest_exchange_time: timestamp,
                    });

                    return WriteAction::Nothing;
                }

                Some(timer) => {
                    timer.newest_exchange_time = timer.newest_exchange_time.max(timestamp);
                    let time_elapsed = received_at - timer.since;
                    if time_elapsed >= TimeDelta::seconds(STALL_TIMER_SECONDS) {
                        if let Some(primary_socket_id) = &self.primary_socket_id {
                            self.socket_orderbook.remove(primary_socket_id);
                            self.create_from_first_update = false;
                        }

                        self.primary_socket_id = Some(socket_id);
                        self.stall_timer = None;
                        return WriteAction::Snapshot;
                    }

                    return WriteAction::Nothing;
                }
            }
        }

        return WriteAction::Nothing;
    }
}
