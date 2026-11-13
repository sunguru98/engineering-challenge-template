use std::collections::HashMap;

use engineering_challenge::util::{
    AlignedSbeWriter,
    dtos::challenge_orderbook::{SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain},
};

use crate::collector::orderbook::{Books, Orderbook};

pub fn write_snapshot(
    symbol: &str,
    orderbook: &Orderbook,
    writer: &mut AlignedSbeWriter,
) -> anyhow::Result<()> {
    let mut asks = vec![];
    for (price_level, quantity) in orderbook.asks.iter() {
        asks.push(SnapshotAsksEntryDomain {
            price_level: *price_level,
            quantity: *quantity,
        })
    }

    let mut bids = vec![];
    for (price_level, quantity) in orderbook.bids.iter().rev() {
        bids.push(SnapshotBidsEntryDomain {
            price_level: *price_level,
            quantity: *quantity,
        })
    }

    let snapshot = SnapshotDomain {
        asks,
        bids,
        exchange_timestamp: orderbook.timestamp,
        received_at: chrono::Utc::now(),
        symbol: symbol.to_owned(),
    };
    writer.write_msg(&snapshot)?;

    Ok(())
}

pub fn take_snapshot(
    symbol_books: &HashMap<String, Books>,
    writer: &mut AlignedSbeWriter,
) -> anyhow::Result<()> {
    for (symbol, books) in symbol_books.iter() {
        let Some(orderbook) = books.primary_book() else {
            eprintln!("Skipping {symbol} since no socket present");
            continue;
        };

        write_snapshot(symbol, orderbook, writer)?;
    }
    Ok(())
}
