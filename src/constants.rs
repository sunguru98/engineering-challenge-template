pub const HTTP_URL: &str = "https://gamma-api.polymarket.com/events/keyset";
pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const MAX_ATTEMPTS: u8 = 50;
pub const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;
pub const WORKER_CHANNEL_SIZE: usize = 50_000;
pub const UPDATE_EVENT_CHANNEL_SIZE: usize = 10_000;
pub const BATCH_SIZE: usize = 2_500;
