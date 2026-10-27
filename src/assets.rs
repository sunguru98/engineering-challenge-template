use std::collections::HashMap;

use crate::orderbook::Orderbook;

pub const BATCH_SIZE: usize = 5_000;

pub fn extract_asset_id(market: &serde_json::Value) -> anyhow::Result<Option<String>> {
    if market["active"].as_bool() != Some(true) {
        return Ok(None);
    }
    let Some(ids) = market["clob_token_ids"].as_array() else {
        return Ok(None);
    };
    if ids.len() != 2 {
        return Ok(None);
    }
    Ok(ids[0].as_str().map(String::from))
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

    for token_id in token_ids.lines().map(String::from) {
        batch.push(token_id.clone());
        asset_batch_map.insert(token_id.clone(), batch_idx);
        if batch_counter == BATCH_SIZE {
            batches.push(batch);
            batch = vec![];
            batch_counter = 0;
            batch_idx += 1;
        } else {
            batch_counter += 1;
        }

        assets_ids.insert(token_id, Orderbook::new());
    }

    if !batch.is_empty() {
        batches.push(batch)
    }

    Ok((assets_ids, batches, asset_batch_map))
}
