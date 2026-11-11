use std::collections::HashMap;

use crate::{
    orderbook::Books,
    polymarket::constants::BATCH_SIZE,
    types::{Batches, SymbolBatchMap, SymbolBooksMap},
};

pub fn fetch_symbols(
    token_ids: Vec<String>,
) -> anyhow::Result<(SymbolBooksMap, Batches, SymbolBatchMap)> {
    let mut assets_ids = std::collections::HashMap::new();

    let mut batches: Vec<Vec<String>> = vec![];
    let mut batch: Vec<String> = vec![];

    let mut batch_idx = 0usize;
    let mut symbol_batch_map = HashMap::new();

    for token_id in token_ids {
        batch.push(token_id.clone());
        symbol_batch_map.insert(token_id.clone(), batch_idx);
        if batch.len() == BATCH_SIZE {
            batches.push(batch);
            batch = vec![];
            batch_idx += 1;
        }

        assets_ids.insert(token_id, Books::new());
    }

    if !batch.is_empty() {
        batches.push(batch)
    }

    Ok((assets_ids, batches, symbol_batch_map))
}
