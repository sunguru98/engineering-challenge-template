# Voluntas Engineering Challenge

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
| `src/main.rs` | Application entry point and orchestration |
| `src/worker.rs` | WebSocket worker and subscription handling |
| `src/handlers.rs` | Incoming market-event processing |
| `src/orderbook.rs` | Orderbook struct and snapshot related functions |
| `src/tokens.rs` | Asset fetching through HTTP cursor based pagination |

The supplied schema and serialization helpers are used to keep the output compatible with the challenge's expected format.

## Build and run

```bash
cargo build --release
```

Run the application:

```bash
cargo run --release > /path_to_data.bin
```