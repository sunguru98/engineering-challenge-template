use std::{collections::HashMap, time::Duration};

use anyhow::{Error, Result};
use chrono::Utc;
use engineering_challenge::util::AlignedSbeWriter;
use tokio::{
    signal,
    sync::{
        mpsc::{self},
        watch::{self},
    },
    task::{JoinError, JoinSet},
    time::{Interval, interval, sleep},
};

use crate::{
    collector::{
        handlers::{add_symbol, apply_level_update, apply_snapshot, remove_symbol},
        orderbook::{Books, WriteAction},
        snapshot::{take_snapshot, write_snapshot},
        subscription::Subscription,
        worker::{IncomingMessage, run_worker},
    },
    constants::{
        CHUNK_SIZE_64_MEBIBYTES, SNAPSHOT_TIME_SECONDS, SOCKETS_PER_BATCH, WORKER_CHANNEL_SIZE,
        WORKER_SLEEP_MS,
    },
    exchanges::{
        messages::{Command, MarketEvent},
        polymarket::BATCH_SIZE,
    },
};

pub mod handlers;
pub mod orderbook;
pub mod snapshot;
pub mod subscription;
pub mod worker;

type JoinsetType = Result<(usize, usize), (usize, usize, Error)>;
pub struct Collector {
    writer: AlignedSbeWriter,
    symbol_books: HashMap<String, Books>,
    subscription: Subscription,

    // Channels
    signal_sender: watch::Sender<bool>,
    signal_receiver: watch::Receiver<bool>,
    main_sender: Option<mpsc::Sender<IncomingMessage>>,
    receiver: mpsc::Receiver<IncomingMessage>,

    // Timers/Spawners
    snapshot_timer: Interval,
    join_set: JoinSet<JoinsetType>,

    // Config variables (Shutdown)
    shutting_down: bool,
    all_events_processed: bool,
    work_complete: bool,
}

impl Collector {
    pub fn new(symbols: Vec<String>) -> Result<Self> {
        let file = std::path::Path::new("/dev/stdout");
        let writer = AlignedSbeWriter::create(file, CHUNK_SIZE_64_MEBIBYTES)?;

        let batches = Subscription::split_into_batches(&symbols, BATCH_SIZE);
        let subscription = Subscription::new(&batches);

        let mut symbol_books = HashMap::new();
        let (sender, receiver) = mpsc::channel::<IncomingMessage>(WORKER_CHANNEL_SIZE);
        let (signal_sender, signal_receiver) = watch::channel(false);
        let interval = interval(Duration::from_secs(SNAPSHOT_TIME_SECONDS));

        let join_set = JoinSet::new();

        for symbol in symbols {
            symbol_books.insert(symbol, Books::new());
        }

        eprintln!("Total assets: {}", symbol_books.len());
        eprintln!("Total batches: {}", batches.len());

        let mut collector = Self {
            writer,
            subscription,
            main_sender: Some(sender.clone()),
            receiver,
            signal_sender,
            signal_receiver,
            symbol_books,
            snapshot_timer: interval,
            join_set,
            shutting_down: false,
            all_events_processed: false,
            work_complete: false,
        };

        for (batch_idx, batch) in batches.into_iter().enumerate() {
            for _ in 0..SOCKETS_PER_BATCH {
                let (new_socket_id, event_receiver) =
                    collector.subscription.register_socket(batch_idx);
                let startup_delay = Duration::from_millis((new_socket_id as u64) * WORKER_SLEEP_MS);

                collector.spawn_worker(
                    startup_delay,
                    new_socket_id,
                    batch_idx,
                    batch.clone(),
                    sender.clone(),
                    event_receiver,
                );
            }
        }

        Ok(collector)
    }

    fn spawn_worker(
        &mut self,
        startup_delay: Duration,
        new_socket_id: usize,
        batch_idx: usize,
        batch: Vec<String>,
        sender: mpsc::Sender<IncomingMessage>,
        event_receiver: mpsc::Receiver<Command>,
    ) {
        let signal_receiver = self.signal_receiver.clone();

        self.join_set.spawn(async move {
            sleep(startup_delay).await;
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

    fn replace_worker(&mut self, batch_idx: usize, dead_socket_id: usize) -> Result<()> {
        if !self.shutting_down {
            eprintln!("Retrying Websocket #{batch_idx}");
            let batch: Vec<String> = self.subscription.symbols_in_batch(batch_idx);
            self.subscription.remove_socket(batch_idx, dead_socket_id);

            if let Some(main_sender) = self.main_sender.clone() {
                let (new_socket_id, event_receiver) = self.subscription.register_socket(batch_idx);

                for asset_id in batch.iter() {
                    let Some(books) = self.symbol_books.get_mut(asset_id) else {
                        continue;
                    };

                    let write_action = books.drop_socket(dead_socket_id);
                    if write_action == WriteAction::Snapshot {
                        if let Some(primary_orderbook) = books.primary_book() {
                            write_snapshot(asset_id, primary_orderbook, &mut self.writer)?;
                        }
                    }
                }

                self.spawn_worker(
                    Duration::ZERO,
                    new_socket_id,
                    batch_idx,
                    batch,
                    main_sender,
                    event_receiver,
                );
            }
        }

        Ok(())
    }

    fn handle_incoming_message(&mut self, incoming_message: IncomingMessage) -> Result<()> {
        for event in incoming_message.market_events {
            match event {
                MarketEvent::Snapshot {
                    symbol,
                    asks,
                    bids,
                    exchange_time,
                } => {
                    if !self
                        .subscription
                        .is_socket_alive(incoming_message.socket_id)
                    {
                        continue;
                    }

                    apply_snapshot(
                        incoming_message.socket_id,
                        &mut self.symbol_books,
                        symbol,
                        exchange_time,
                        bids,
                        asks,
                    );
                }

                MarketEvent::LevelUpdate {
                    exchange_time,
                    new_quantity,
                    price,
                    side,
                    symbol,
                } => {
                    if !self
                        .subscription
                        .is_socket_alive(incoming_message.socket_id)
                    {
                        continue;
                    }

                    apply_level_update(
                        incoming_message.socket_id,
                        &mut self.symbol_books,
                        symbol,
                        &mut self.writer,
                        exchange_time,
                        side,
                        price,
                        new_quantity,
                        incoming_message.received_at,
                    )?;
                }

                MarketEvent::SymbolAdded { symbol } => {
                    add_symbol(symbol, &mut self.symbol_books, &mut self.subscription);
                }

                MarketEvent::SymbolRemoved { symbol } => {
                    remove_symbol(symbol, &mut self.symbol_books, &mut self.subscription);
                }
            }
        }

        Ok(())
    }

    fn begin_shutdown(&mut self) -> Result<()> {
        eprintln!("\nCommencing Shutdown");
        self.signal_sender.send(true)?;
        self.shutting_down = true;
        self.main_sender = None;

        Ok(())
    }

    fn snapshot(&mut self) -> Result<()> {
        eprintln!("Creating a snapshot at {}", Utc::now());
        take_snapshot(&self.symbol_books, &mut self.writer)?;

        Ok(())
    }

    fn handle_worker_result(
        &mut self,
        join_result: Option<Result<JoinsetType, JoinError>>,
    ) -> Result<()> {
        match join_result {
            None => {
                if self.shutting_down {
                    eprintln!("All websockets closed");
                    self.work_complete = true;
                }
            }
            Some(Ok(Ok((batch_idx, socket_id)))) => {
                eprintln!("Websocket #{socket_id} for batch #{batch_idx} finished running")
            }
            Some(Ok(Err((err_socket_id, batch_idx, err)))) => {
                eprintln!(
                    "Websocket #{err_socket_id} for batch #{batch_idx} finished with error: {err:?}"
                );
                self.replace_worker(batch_idx, err_socket_id)?;
            }
            Some(Err(err)) => eprintln!("Task returned with error: {err:?}"),
        }

        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        let sigint = signal::ctrl_c();
        tokio::pin!(sigint);

        self.snapshot_timer.tick().await;

        loop {
            let can_shutdown =
                self.shutting_down && self.all_events_processed && self.work_complete;
            let can_view_join_result =
                !self.work_complete && (self.shutting_down || !self.join_set.is_empty());

            if can_shutdown {
                break;
            }

            tokio::select! {
                _ = &mut sigint, if !self.shutting_down => self.begin_shutdown()?,
                _ = self.snapshot_timer.tick() => self.snapshot()?,
                join_result = self.join_set.join_next(), if can_view_join_result => self.handle_worker_result(join_result)?,

                incoming_message = self.receiver.recv(), if !self.all_events_processed => {
                    match incoming_message {
                        None => {
                            self.all_events_processed = true;
                        }
                        Some(incoming_message) => self.handle_incoming_message(incoming_message)?
                    }
                }
            }
        }

        self.writer.flush()?;
        Ok(())
    }
}
