use rand::RngExt;

pub const URL: &str = "https://gamma-api.polymarket.com/events/keyset";

pub async fn fetch_page(
    client: &reqwest::Client,
    query_params: &std::collections::HashMap<&str, String>,
) -> anyhow::Result<(Vec<String>, Option<String>)> {
    let response = client.get(URL).query(query_params).send().await?;
    let response = response.error_for_status()?;

    let data = response.json::<serde_json::Value>().await?;
    let next_cursor = data["next_cursor"].as_str().map(str::to_owned);

    let events = data["events"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Events not found"))?;

    let mut token_ids: Vec<String> = vec![];
    for event in events {
        let markets = event["markets"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Markets not found"))?;

        for market in markets {
            let is_active = market["active"].as_bool();
            let is_closed = market["closed"].as_bool();
            let is_orderbook_enabled = market["enableOrderBook"].as_bool();
            let is_accepting_orders = market["acceptingOrders"].as_bool();

            if is_active == Some(true)
                && is_closed == Some(false)
                && is_accepting_orders == Some(true)
                && is_orderbook_enabled == Some(true)
            {
                let Some(clob_token_ids) = market["clobTokenIds"].as_str() else {
                    continue;
                };

                let Ok(mut clob_token_ids) = serde_json::from_str::<Vec<String>>(clob_token_ids)
                else {
                    continue;
                };

                if clob_token_ids.len() != 2 {
                    continue;
                }

                token_ids.push(clob_token_ids.swap_remove(0));
            }
        }
    }

    println!("{} tokens from {} events", token_ids.len(), events.len());

    println!("Next cursor: {next_cursor:?}");
    return Ok((token_ids, next_cursor));
}

pub async fn fetch_markets(
    client: &reqwest::Client,
    query_params: &std::collections::HashMap<&str, String>,
) -> anyhow::Result<(Vec<String>, Option<String>)> {
    let mut attempt_number = 0;
    let mut delay_seconds = std::time::Duration::from_secs(1);
    let maximum_delay_seconds = std::time::Duration::from_secs(30);

    loop {
        if attempt_number > 20 {
            anyhow::bail!("Connection failed");
        }

        match fetch_page(client, query_params).await {
            Ok(result) => return Ok(result),
            Err(err) => {
                eprintln!("Error: {err:?}");
                eprintln!("Connection failed. Retrying #{attempt_number}");
            }
        };

        let jitter_ms = std::time::Duration::from_millis(rand::rng().random_range(0..750));
        let waiting_seconds = delay_seconds + jitter_ms;
        tokio::time::sleep(waiting_seconds).await;

        delay_seconds = (delay_seconds * 2).min(maximum_delay_seconds);
        attempt_number += 1;
    }
}

pub async fn fetch_all_markets(buf_writer: &mut impl std::io::Write) -> anyhow::Result<()> {
    let mut query_params: std::collections::HashMap<&str, String> =
        std::collections::HashMap::new();
    query_params.insert("closed", "false".to_string());
    query_params.insert("limit", "100".to_string());

    let client = reqwest::Client::new();

    loop {
        let (token_ids, next_cursor) = fetch_markets(&client, &query_params).await?;
        for id in token_ids {
            writeln!(buf_writer, "{id}")?;
        }

        buf_writer.flush()?;

        let Some(next_cursor) = next_cursor.filter(|c| !c.is_empty()) else {
            break;
        };
        query_params.insert("after_cursor", next_cursor);
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let file = std::fs::File::create("token_ids.txt.tmp")?;
    let mut buf_writer = std::io::BufWriter::new(file);
    fetch_all_markets(&mut buf_writer).await?;

    std::fs::rename("token_ids.txt.tmp", "token_ids.txt")?;

    Ok(())
}
