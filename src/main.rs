use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use chrono::Utc;
use engineering_challenge::util::AlignedSbeWriter;
use tokio::{
    signal,
    sync::{mpsc, watch},
    time::interval,
};

mod assets;
mod constants;
mod handlers;
mod orderbook;
mod parse;
mod tokens;
mod types;
mod worker;

use assets::fetch_asset_ids;
use constants::CHUNK_SIZE_64_MEBIBYTES;
use orderbook::take_snapshot;
use tokens::fetch_all_markets;
use types::EType;
use worker::run_worker;

use crate::constants::{SNAPSHOT_TIME_SECONDS, UPDATE_EVENT_CHANNEL_SIZE, WORKER_CHANNEL_SIZE};

#[tokio::main]
async fn main() -> Result<()> {
    let token_ids = fetch_all_markets().await?;
    eprintln!("Fetched {} tokens/asset_ids from events", token_ids.len());

    let file = std::path::Path::new("/dev/stdout");
    let mut writer = AlignedSbeWriter::create(file, CHUNK_SIZE_64_MEBIBYTES)?;

    let (mut assets_ids, batches, mut asset_worker_map) = fetch_asset_ids(token_ids)?;
    eprintln!("Total assets: {}", assets_ids.len());
    let batches_len = batches.len();
    eprintln!("Total batches: {batches_len}");

    let mut batch_sender_map = HashMap::new();

    let (sender, mut receiver) = mpsc::channel(WORKER_CHANNEL_SIZE);
    let (signal_sender, signal_receiver) = watch::channel(false);

    let mut join_set = tokio::task::JoinSet::new();
    for (batch_idx, batch) in batches.into_iter().enumerate() {
        let (event_sender, event_receiver) = mpsc::channel(UPDATE_EVENT_CHANNEL_SIZE);
        batch_sender_map.insert(batch_idx, event_sender);

        let signal_receiver = signal_receiver.clone();
        let sender = sender.clone();

        join_set.spawn(run_worker(
            batch_idx,
            batch,
            sender,
            event_receiver,
            signal_receiver,
        ));
    }
    drop(sender);

    let mut interval = interval(Duration::from_secs(SNAPSHOT_TIME_SECONDS));
    interval.tick().await;

    let mut shutting_down = false;
    let mut all_events_processed = false;
    let mut work_complete = false;
    let mut next_batch_idx = 0;

    let sigint = signal::ctrl_c();
    tokio::pin!(sigint);
    loop {
        if shutting_down && all_events_processed && work_complete {
            break;
        }

        tokio::select! {
            _ = &mut sigint, if !shutting_down => {
                eprintln!("\nCommencing Shutdown");
                signal_sender.send(true)?;
                shutting_down = true;
            }

            _ = interval.tick() => {
                eprintln!("Creating a snapshot at {}", Utc::now());
                take_snapshot(&assets_ids, &mut writer)?;
            }

            join_result = join_set.join_next(), if shutting_down && !work_complete => {
                match join_result {
                    Some(Ok(Ok(batch_idx))) => eprintln!("Websocket #{batch_idx} finished running"),
                    Some(Ok(Err(err))) => eprintln!("Websocket finished with error: {err:?}"),
                    Some(Err(err)) => eprintln!("Task returned with error: {err:?}"),
                    None => {
                        eprintln!("All websockets closed");
                        work_complete = true
                    }
                }
            }

            event = receiver.recv(), if !all_events_processed => {
                match event {
                    None => {
                        all_events_processed = true;
                    }
                    Some(event) => {
                        let e_data = event.data;
                        match event.e_type {
                            EType::MarketResolved => {
                                if let Err(err) = handlers::handle_market_resolved(
                                    &mut assets_ids,
                                    &mut asset_worker_map,
                                    &batch_sender_map,
                                    &e_data,
                                )
                                .await
                                {
                                    eprintln!("Skipping bad market_resolved event: {err:#}");
                                }
                            }
                            EType::NewMarket => {
                                if let Err(err) = handlers::handle_new_market(
                                    &mut assets_ids,
                                    &mut asset_worker_map,
                                    &batch_sender_map,
                                    &mut next_batch_idx,
                                    batches_len,
                                    &e_data,
                                )
                                .await
                                {
                                    eprintln!("Skipping bad new_market event: {err:#}");
                                }
                            }
                            EType::Book => {
                                if let Err(err) = handlers::handle_book(&mut assets_ids, &e_data) {
                                    eprintln!("Skipping bad book event: {err:#}");
                                }
                            }
                            EType::PriceChange => {
                                if let Err(err) = handlers::handle_price_change(
                                    &mut assets_ids,
                                    &mut writer,
                                    &e_data
                                ) {
                                    eprintln!("Skipping bad price_change event: {err:#}");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    writer.flush()?;
    Ok(())
}
