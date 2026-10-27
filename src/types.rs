#[derive(serde::Serialize)]
pub struct SubscriptionPayload {
    pub assets_ids: Vec<String>,
    pub r#type: &'static str,
    pub custom_feature_enabled: bool,
    pub initial_dump: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PriceData {
    pub price: String,
    pub size: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct BookEvent {
    pub asset_id: String,
    pub timestamp: String,
    pub bids: Vec<PriceData>,
    pub asks: Vec<PriceData>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PriceChangeEvent {
    pub timestamp: String,
    pub price_changes: Vec<PriceChange>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct MarketResolvedEvent {
    pub assets_ids: Vec<String>,
    pub timestamp: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PriceChange {
    pub asset_id: String,
    pub price: String,
    pub side: String,
    pub size: String,
}

#[derive(Debug)]
pub enum EType {
    Book,
    PriceChange,
    NewMarket,
    MarketResolved,
}

#[derive(Debug)]
pub struct Event {
    pub e_type: EType,
    pub data: serde_json::Value,
}

#[derive(Debug)]
pub enum UpdateEvents {
    MarketResolved { asset_ids: Vec<String> },
    NewMarket { asset_ids: Vec<String> },
}
