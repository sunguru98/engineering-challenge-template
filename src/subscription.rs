use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{constants::UPDATE_EVENT_CHANNEL_SIZE, exchanges::messages::Command};
use std::collections::{HashMap, HashSet};

pub struct Subscription {
    symbol_batch_map: HashMap<String, usize>,
    batch_socket_ids_map: HashMap<usize, HashSet<usize>>,
    socket_event_sender_map: HashMap<usize, Sender<Command>>,
    next_batch_idx: usize,
    number_of_batches: usize,
    next_socket_id: usize,
}

impl Subscription {
    pub fn new(batches: &[Vec<String>]) -> Self {
        let mut symbol_batch_map: HashMap<String, usize> = HashMap::new();
        for (batch_idx, batch) in batches.iter().enumerate() {
            for symbol in batch {
                symbol_batch_map.insert(symbol.clone(), batch_idx);
            }
        }

        Self {
            symbol_batch_map,
            batch_socket_ids_map: HashMap::new(),
            socket_event_sender_map: HashMap::new(),
            next_batch_idx: 0,
            number_of_batches: batches.len(),
            next_socket_id: 0,
        }
    }

    pub fn split_into_batches(symbols: &[String], batch_size: usize) -> Vec<Vec<String>> {
        symbols
            .chunks(batch_size)
            .map(|chunk| chunk.to_vec())
            .collect()
    }

    pub fn is_socket_alive(&self, socket_id: usize) -> bool {
        self.socket_event_sender_map.contains_key(&socket_id)
    }

    pub fn symbols_in_batch(&self, batch_idx: usize) -> Vec<String> {
        self.symbol_batch_map
            .iter()
            .filter(|&(_, &idx)| idx == batch_idx)
            .map(|(asset, _)| asset.clone())
            .collect()
    }

    pub fn broadcast_to_batch(&self, batch_idx: usize, command: Command) {
        let Some(socket_ids) = self.batch_socket_ids_map.get(&batch_idx) else {
            eprintln!("No websockets found for batch #{batch_idx}");
            return;
        };

        for socket_id in socket_ids {
            let Some(event_sender) = self.socket_event_sender_map.get(socket_id) else {
                eprintln!("Socket #{socket_id} not found");
                continue;
            };

            if let Err(err) = event_sender.try_send(command.clone()) {
                eprintln!("Error sending batch #{batch_idx} event: {err:?}");
                continue;
            };
        }
    }

    pub fn assign_symbol(&mut self, symbol: &str) -> Option<usize> {
        if self.number_of_batches == 0 {
            return None;
        }

        let batch_idx = self.next_batch_idx;
        self.symbol_batch_map.insert(symbol.to_owned(), batch_idx);
        self.next_batch_idx = (batch_idx + 1) % self.number_of_batches;

        Some(batch_idx)
    }

    pub fn release_symbol(&mut self, symbol: &str) -> Option<usize> {
        self.symbol_batch_map.remove(symbol)
    }

    pub fn register_socket(&mut self, batch_idx: usize) -> (usize, Receiver<Command>) {
        let current_socket_id = self.next_socket_id;
        let (event_sender, event_receiver) = mpsc::channel(UPDATE_EVENT_CHANNEL_SIZE);

        let socket_ids = self.batch_socket_ids_map.entry(batch_idx).or_default();
        socket_ids.insert(current_socket_id);

        self.socket_event_sender_map
            .insert(self.next_socket_id, event_sender);
        self.next_socket_id += 1;

        (current_socket_id, event_receiver)
    }

    pub fn remove_socket(&mut self, batch_idx: usize, socket_id: usize) {
        self.socket_event_sender_map.remove(&socket_id);
        if let Some(socket_ids) = self.batch_socket_ids_map.get_mut(&batch_idx) {
            socket_ids.remove(&socket_id);
        }
    }
}
