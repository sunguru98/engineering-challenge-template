use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use tokio::{
    sync::{mpsc, watch},
    time::{interval, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    constants::{MAX_ATTEMPTS, STABLE_CHECK_SECONDS},
    exchanges::{
        messages::{Command, MarketEvent},
        polymarket::{
            HEARTBEAT_TIMER_SECONDS, WS_URL, decode, encode_command, encode_heartbeat_message,
            encode_initial_subscribe,
        },
    },
};

pub struct IncomingMessage {
    pub socket_id: usize,
    pub received_at: DateTime<Utc>,
    pub market_events: Vec<MarketEvent>,
}

pub async fn run_worker(
    socket_id: usize,
    batch_idx: usize,
    batch: Vec<String>,
    sender: mpsc::Sender<IncomingMessage>,
    mut event_receiver: mpsc::Receiver<Command>,
    mut signal_receiver: watch::Receiver<bool>,
) -> anyhow::Result<(usize, usize)> {
    let mut event_sender_closed = false;
    let mut attempt_number = 1;
    let mut delay_seconds = std::time::Duration::from_secs(1);
    let maximum_delay_seconds = std::time::Duration::from_secs(30);
    let mut worker_symbols: HashSet<String> = HashSet::new();
    let mut heartbeat_interval = interval(Duration::from_secs(HEARTBEAT_TIMER_SECONDS));

    for key in batch.into_iter() {
        worker_symbols.insert(key);
    }
    heartbeat_interval.tick().await;

    loop {
        let connected_at = std::time::Instant::now();

        let result: anyhow::Result<()> = async {
            let (stream, _) = connect_async(
                WS_URL,
            )
            .await?;

            let (mut write, mut read) = stream.split();

            let payload = encode_initial_subscribe(&worker_symbols);

            write
                .send(Message::Text(
                    payload.into(),
                ))
                .await?;

            eprintln!("Subscription done for websocket #{socket_id} for batch #{batch_idx}");
            loop {
                tokio::select! {
                    _ = signal_receiver.changed() => {
                        if *signal_receiver.borrow() {
                            if let Err(err) = write.send(Message::Close(None)).await {
                                eprintln!("Shutdown close frame error: {err:?}");
                                break;
                            }

                            let timeout_res = timeout(Duration::from_secs(2), async {
                                while let Some(response) = read.next().await {
                                    match response {
                                        Ok(Message::Close(_)) =>  {
                                            eprintln!("Websocket #{socket_id} for #{batch_idx} closed succesfully");
                                            break;
                                        }
                                        Ok(_) => continue,
                                        Err(_) => break,
                                    }
                                }
                            }).await;

                            if timeout_res.is_err() {
                                eprintln!("Websocket #{socket_id} for #{batch_idx} shutdown reply timed out")
                            }

                            break;
                        }
                    }
                    _ = heartbeat_interval.tick() => {
                        write.send(encode_heartbeat_message()).await?;
                    }

                    command = event_receiver.recv(), if !event_sender_closed => {
                        match command {
                            Some(command) => {
                                let payload = encode_command(&command);
                                match command {
                                    Command::Unsubscribe { symbols } => {
                                        write.send(payload.into()).await?;
                                        eprintln!("Unsubscribed market of {symbols:?} succesfully");
                                        for id in symbols {
                                            worker_symbols.remove(&id);
                                        }
                                    },
                                    Command::Subscribe { symbols } => {
                                        write.send(payload.into()).await?;
                                        eprintln!("New market of {symbols:?} added succesfully");
                                        for id in symbols {
                                            worker_symbols.insert(id);
                                        }
                                    }
                                }
                            },
                            None => {
                                event_sender_closed = true
                            }
                        }
                    }

                    response = read.next() => {
                        let Some(response) = response else {
                            anyhow::bail!("Stream ended unxpectedly");
                        };

                        match response {
                            Ok(Message::Text(text)) => {
                                let received_at = Utc::now();
                                let market_events = decode(&text);
                                if !market_events.is_empty() {
                                    sender.send(IncomingMessage {
                                        market_events,
                                        received_at,
                                        socket_id,
                                    }).await?;
                                }
                            }
                            Ok(other_msg) => eprintln!("Other message received: {other_msg}"),
                            Err(e) => anyhow::bail!(e),
                        }
                    }
                }
            }
            Ok(())
        }.await;

        match result {
            Ok(()) => break,
            Err(err) => eprintln!("Connection error: {err}"),
        }

        if connected_at.elapsed() > Duration::from_secs(STABLE_CHECK_SECONDS) {
            attempt_number = 0;
            delay_seconds = Duration::from_secs(1);
        }

        let jitter_ms = Duration::from_millis(rand::rng().random_range(0..750));
        let waiting_seconds = delay_seconds + jitter_ms;
        tokio::select! {
            _ = signal_receiver.changed() => {
                if *signal_receiver.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(waiting_seconds) => {}
        }

        delay_seconds = (delay_seconds * 2).min(maximum_delay_seconds);
        attempt_number += 1;

        if attempt_number > MAX_ATTEMPTS {
            anyhow::bail!("Connection failed");
        }
    }

    Ok((batch_idx, socket_id))
}
