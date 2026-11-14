use std::time::Duration;

use rand::RngExt;

use crate::constants::MAX_ATTEMPTS;

const JITTER_MS: u64 = 750;
const MAX_DELAY_SECONDS: u64 = 30;
const INITIAL_DELAY_SECONDS: u64 = 1;

pub struct Backoff {
    max_delay: Duration,
    attempt: u8,
    delay: Duration,
}

impl Backoff {
    pub fn new() -> Self {
        Self {
            max_delay: Duration::from_secs(MAX_DELAY_SECONDS),
            attempt: 1,
            delay: Duration::from_secs(INITIAL_DELAY_SECONDS),
        }
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
        self.delay = Duration::from_secs(INITIAL_DELAY_SECONDS);
    }

    pub fn wait_time(&self) -> Duration {
        let jitter_ms = Duration::from_millis(rand::rng().random_range(0..JITTER_MS));
        self.delay + jitter_ms
    }

    pub fn record_failure(&mut self) {
        self.delay = (self.delay * 2).min(self.max_delay);
        self.attempt += 1;
    }

    pub fn is_exhausted(&self) -> bool {
        self.attempt > MAX_ATTEMPTS
    }
}
