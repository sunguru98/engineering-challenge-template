use std::time::Duration;
use std::{collections::HashSet, time::Instant};

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    time::{Interval, interval, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::Message::{self, Text},
};

use crate::{
    backoff::Backoff,
    constants::{CLOSE_REPLY_TIMEOUT_SECONDS, SOCKET_LIVENESS_SECONDS, STABLE_CHECK_SECONDS},
    exchanges::{
        adapter::Adapter,
        messages::{Command, MarketEvent},
    },
};

type WsType = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct IncomingMessage {
    pub socket_id: usize,
    pub batch_idx: usize,
    pub received_at: DateTime<Utc>,
    pub market_events: Vec<MarketEvent>,
}
pub struct Worker<Exchange> {
    exchange: Exchange,
    socket_id: usize,
    batch_idx: usize,
    sender: mpsc::Sender<IncomingMessage>,
    command_receiver: mpsc::Receiver<Command>,
    signal_receiver: watch::Receiver<bool>,
    symbols: HashSet<String>,
    heartbeat_timer: Interval,
    backoff: Backoff,
    command_closed: bool,
    session_established: bool,
    last_message_at: Option<Instant>,
}

impl<Exchange> Worker<Exchange>
where
    Exchange: Adapter,
{
    pub fn new(
        exchange: Exchange,
        socket_id: usize,
        batch_idx: usize,
        batch: Vec<String>,
        sender: mpsc::Sender<IncomingMessage>,
        command_receiver: mpsc::Receiver<Command>,
        signal_receiver: watch::Receiver<bool>,
    ) -> Self {
        let symbols = batch.into_iter().collect::<HashSet<String>>();
        let heartbeat_timer = interval(exchange.heartbeat_interval());
        Self {
            exchange,
            backoff: Backoff::new(),
            batch_idx,
            command_receiver,
            heartbeat_timer,
            sender,
            signal_receiver,
            socket_id,
            symbols,
            command_closed: false,
            session_established: false,
            last_message_at: None,
        }
    }

    pub async fn run(mut self) -> Result<(usize, usize)> {
        self.heartbeat_timer.tick().await;

        loop {
            let connected_at = Instant::now();
            match self.run_session().await {
                Ok(()) => break,
                Err(err) => {
                    eprintln!("Connection error: {err}");
                    if self.session_established {
                        self.session_established = false;
                        self.sender
                            .send(IncomingMessage {
                                socket_id: self.socket_id,
                                batch_idx: self.batch_idx,
                                received_at: Utc::now(),
                                market_events: vec![MarketEvent::Disconnected],
                            })
                            .await?;
                    }
                }
            }

            if connected_at.elapsed() > Duration::from_secs(STABLE_CHECK_SECONDS) {
                self.backoff.reset();
            }

            tokio::select! {
                _ = self.signal_receiver.changed() => {
                    if *self.signal_receiver.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.backoff.wait_time()) => {}
            }

            self.backoff.record_failure();
            if self.backoff.is_exhausted() {
                bail!("Connection failed");
            }
        }

        Ok((self.batch_idx, self.socket_id))
    }

    async fn run_session(&mut self) -> Result<()> {
        let (stream, _) = connect_async(self.exchange.websocket_url()).await?;
        let (mut write, mut read) = stream.split();
        let payload = self.exchange.initial_subscribe(&self.symbols);
        write.send(Message::Text(payload.into())).await?;

        eprintln!(
            "Subscription done for websocket #{} for batch #{}",
            self.socket_id, self.batch_idx
        );

        self.last_message_at = Some(Instant::now());
        self.session_established = true;

        loop {
            tokio::select! {
                _ = self.signal_receiver.changed() => {
                    if *self.signal_receiver.borrow() {
                        self.close_connection(write, read).await;
                        break;
                    }
                }
                _ = self.heartbeat_timer.tick() => {
                    if let Some(last_message_at) = self.last_message_at {
                        let elapsed_time = last_message_at.elapsed();
                        if elapsed_time >= Duration::from_secs(SOCKET_LIVENESS_SECONDS) {
                            bail!("No new message since {elapsed_time:?}");
                        }
                    }

                    write.send(self.exchange.heartbeat_message()).await?;
                }

                command = self.command_receiver.recv(), if !self.command_closed => {
                    match command {
                        Some(command) => {
                            let payload = self.handle_command(command);
                            write.send(payload.into()).await?;
                        },
                        None => {
                            self.command_closed = true
                        }
                    }
                }

                response = read.next() => {
                    let Some(response) = response else {
                        bail!("Stream ended unxpectedly");
                    };

                    match response {
                        Ok(message) => {
                            self.last_message_at = Some(Instant::now());
                            self.handle_message(message).await?
                        },
                        Err(e) => bail!(e),
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_message(&self, message: Message) -> Result<()> {
        match message {
            Text(text) => {
                let received_at = Utc::now();
                let market_events = self.exchange.decode(&text);
                if !market_events.is_empty() {
                    self.sender
                        .send(IncomingMessage {
                            socket_id: self.socket_id,
                            batch_idx: self.batch_idx,
                            market_events,
                            received_at,
                        })
                        .await?;
                }
            }

            other_msg => eprintln!("Other message received: {other_msg}"),
        }

        Ok(())
    }

    async fn close_connection(
        &self,
        mut write: SplitSink<WsType, Message>,
        mut read: SplitStream<WsType>,
    ) {
        if let Err(err) = write.send(Message::Close(None)).await {
            eprintln!("Shutdown close frame error: {err:?}");
            return;
        }

        let timeout_res = timeout(Duration::from_secs(CLOSE_REPLY_TIMEOUT_SECONDS), async {
            while let Some(response) = read.next().await {
                match response {
                    Ok(Message::Close(_)) => {
                        eprintln!(
                            "Websocket #{} for #{} closed succesfully",
                            self.socket_id, self.batch_idx
                        );
                        break;
                    }
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        })
        .await;

        if timeout_res.is_err() {
            eprintln!(
                "Websocket #{} for #{} shutdown reply timed out",
                self.socket_id, self.batch_idx
            );
        }
    }

    fn handle_command(&mut self, command: Command) -> String {
        let payload = self.exchange.encode_command(&command);
        match command {
            Command::Unsubscribe { symbols } => {
                eprintln!("Sending Unsubscribe market of {symbols:?}");
                for id in symbols {
                    self.symbols.remove(&id);
                }
            }
            Command::Subscribe { symbols } => {
                eprintln!("Sending subscribe market of {symbols:?}");
                for id in symbols {
                    self.symbols.insert(id);
                }
            }
        }

        payload
    }
}
