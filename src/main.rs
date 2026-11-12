use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use chrono::Utc;
use engineering_challenge::util::AlignedSbeWriter;
use tokio::{
    signal,
    sync::{mpsc, watch},
    task::JoinSet,
    time::{interval, sleep},
};

mod constants;
mod exchanges;
mod handlers;
mod orderbook;
mod parse;
mod subscription;
mod worker;

use constants::CHUNK_SIZE_64_MEBIBYTES;
use messages::MarketEvent;
use orderbook::take_snapshot;
use worker::run_worker;

use crate::{
    constants::{SNAPSHOT_TIME_SECONDS, SOCKETS_PER_BATCH, WORKER_CHANNEL_SIZE, WORKER_SLEEP_MS},
    exchanges::{polymarket::BATCH_SIZE, *},
    handlers::{add_symbol, apply_level_update, apply_snapshot, remove_symbol},
    orderbook::{Books, write_snapshot},
    polymarket::discovery::*,
    subscription::Subscription,
};

#[tokio::main]
async fn main() -> Result<()> {
    // ------ All main variables -----------------------------------------------
    let symbols = fetch_all_markets().await?;
    let file = std::path::Path::new("/dev/stdout");
    let mut writer = AlignedSbeWriter::create(file, CHUNK_SIZE_64_MEBIBYTES)?;
    let batches = Subscription::split_into_batches(&symbols, BATCH_SIZE);
    let mut symbol_books_map = HashMap::new();
    let mut subscription = Subscription::new(&batches);

    let (sender, mut receiver) = mpsc::channel(WORKER_CHANNEL_SIZE);
    let (signal_sender, signal_receiver) = watch::channel(false);
    let mut interval = interval(Duration::from_secs(SNAPSHOT_TIME_SECONDS));

    let mut shutting_down = false;
    let mut all_events_processed = false;
    let mut work_complete = false;

    let sigint = signal::ctrl_c();
    let mut join_set = JoinSet::new();
    // -----------------------------------------------------------------------

    for symbol in symbols {
        symbol_books_map.insert(symbol, Books::new());
    }

    tokio::pin!(sigint);
    interval.tick().await;

    eprintln!("Total assets: {}", symbol_books_map.len());
    eprintln!("Total batches: {}", batches.len());
    for (batch_idx, batch) in batches.into_iter().enumerate() {
        for _ in 0..SOCKETS_PER_BATCH {
            let batch = batch.clone();
            let signal_receiver = signal_receiver.clone();
            let sender = sender.clone();

            let (new_socket_id, event_receiver) = subscription.register_socket(batch_idx);
            join_set.spawn(async move {
                sleep(Duration::from_millis(
                    (new_socket_id as u64) * WORKER_SLEEP_MS,
                ))
                .await;
                run_worker(
                    new_socket_id,
                    batch_idx,
                    batch,
                    sender,
                    event_receiver,
                    signal_receiver,
                )
                .await
                .map_err(|e| (new_socket_id, batch_idx, e))
            });
        }
    }

    let mut main_sender = Some(sender);
    loop {
        let can_shutdown = shutting_down && all_events_processed && work_complete;
        let can_view_join_result = !work_complete && (shutting_down || !join_set.is_empty());

        if can_shutdown {
            break;
        }

        tokio::select! {
            _ = &mut sigint, if !shutting_down => {
                eprintln!("\nCommencing Shutdown");
                signal_sender.send(true)?;
                shutting_down = true;
                main_sender = None;
            }

            _ = interval.tick() => {
                eprintln!("Creating a snapshot at {}", Utc::now());
                take_snapshot(&symbol_books_map, &mut writer)?;
            }

            join_result = join_set.join_next(), if can_view_join_result => {
                match join_result {
                    None => {
                        if shutting_down {
                            eprintln!("All websockets closed");
                            work_complete = true
                        }
                    }
                    Some(Ok(Ok((batch_idx, socket_id)))) => eprintln!("Websocket #{socket_id} for batch #{batch_idx} finished running"),
                    Some(Ok(Err((err_socket_id, batch_idx, err)))) => {
                        eprintln!("Websocket #{err_socket_id} for batch #{batch_idx} finished with error: {err:?}");
                        if !shutting_down {
                            eprintln!("Retrying Websocket #{batch_idx}");
                            let batch: Vec<String> = subscription.symbols_in_batch(batch_idx);
                            subscription.remove_socket(batch_idx, err_socket_id);

                            if let Some(main_sender) = main_sender.clone() {
                                let (new_socket_id, event_receiver) = subscription.register_socket(batch_idx);

                                for asset_id in batch.iter() {
                                    let Some(books) = symbol_books_map.get_mut(asset_id) else {
                                        continue
                                    };

                                    books.stalled_since = None;

                                    let is_primary_socket = books.primary_socket_id == Some(err_socket_id);
                                    books.socket_orderbook.remove(&err_socket_id);
                                    if !is_primary_socket {
                                        continue;
                                    }

                                    match books.socket_orderbook.keys().next() {
                                        Some(other_socket_id) => {
                                            books.primary_socket_id = Some(*other_socket_id);
                                            let Some(other_orderbook) = books.socket_orderbook.get(other_socket_id) else {
                                                eprintln!("Unable to take snapshot of {other_socket_id}'s orderbook");
                                                continue;
                                            };

                                            write_snapshot(asset_id, other_orderbook, &mut writer)?;
                                        },
                                        None => {
                                            books.primary_socket_id = None;
                                        }
                                    };

                                }

                                let signal_receiver = signal_receiver.clone();
                                join_set.spawn(async move {
                                    run_worker(
                                        new_socket_id,
                                        batch_idx,
                                        batch,
                                        main_sender,
                                        event_receiver,
                                        signal_receiver,
                                    )
                                    .await
                                    .map_err(|e| (new_socket_id, batch_idx, e))
                                });
                            }
                        }
                    },
                    Some(Err(err)) => eprintln!("Task returned with error: {err:?}"),
                }
            }

            incoming_message = receiver.recv(), if !all_events_processed => {
                match incoming_message {
                    None => {
                        all_events_processed = true;
                    }

                    Some(incoming_message) => {
                        for event in incoming_message.market_events {
                            match event {
                                MarketEvent::Snapshot {
                                    symbol,
                                    asks,
                                    bids,
                                    exchange_time,
                                } => {
                                    if !subscription.is_socket_alive(incoming_message.socket_id) {
                                        continue;
                                    }

                                    apply_snapshot(
                                        incoming_message.socket_id,
                                        &mut symbol_books_map,
                                        symbol,
                                        exchange_time,
                                        bids,
                                        asks,
                                    )?;
                                }

                                MarketEvent::LevelUpdate {
                                    exchange_time,
                                    new_quantity,
                                    price,
                                    side,
                                    symbol,
                                } => {
                                    if !subscription.is_socket_alive(incoming_message.socket_id) {
                                        continue;
                                    }

                                    apply_level_update(
                                        incoming_message.socket_id,
                                        &mut symbol_books_map,
                                        symbol,
                                        &mut writer,
                                        exchange_time,
                                        side,
                                        price,
                                        new_quantity,
                                        incoming_message.received_at,
                                    )?;
                                }

                                MarketEvent::SymbolAdded { symbol } => {
                                    add_symbol(
                                        symbol,
                                        &mut symbol_books_map,
                                        &mut subscription,
                                    );
                                }

                                MarketEvent::SymbolRemoved { symbol } => {
                                    remove_symbol(
                                        symbol,
                                        &mut symbol_books_map,
                                        &mut subscription,
                                    );
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
