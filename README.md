# Voluntas Engineering Challenge

Records Polymarket order books to a binary file in the challenge's SBE format. It tracks the first token id of every active, order-book-enabled binary market.

## Architecture (Code flow)

```text
              Polymarket
                   |
            Asset fetching
                   |
            Asset batches
                   |
          WebSocket workers (tokio join_set)
                   |
            Market events
                   |
          Event processing
                   |
           Order-book state
              /         \
             /           \
     Price Change       Full snapshot
        Events         every 60 seconds
          |                  |
          |                  |
          +--------+---------+
                   |
      Coordinated Shutdown (SIGINT)
                   |
           SBE serialization
                   |
                stdout
                   |
            Captured data file
```

## Core logic files

| File | Function |
|---|---|
| `src/main.rs` | Entry point |
| `src/collector/mod.rs` | Main loop, worker spawning, shutdown |
| `src/collector/worker.rs` | One websocket connection: reconnect, heartbeat, liveness check |
| `src/collector/orderbook.rs` | Order books per symbol, primary selection, failover |
| `src/collector/handlers.rs` | Applies market events to the books and writes output |
| `src/collector/snapshot.rs` | Snapshot writing |
| `src/collector/subscription.rs` | Symbol to batch mapping, command routing to sockets |
| `src/exchanges/adapter.rs` | Trait an exchange has to implement |
| `src/exchanges/polymarket/` | Asset discovery, message decoding and encoding |

## Build and run

```bash
cargo build --release
```

Run the application. Logs go to stderr, and Ctrl-C closes the sockets and flushes the output:

```bash
cargo run --release > /path_to_data.bin
```