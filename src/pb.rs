// Hand-written prost types for ws.md (only the messages this client uses).
// No protoc / build.rs needed. Field tags must match message.proto / common.proto.
#![allow(dead_code)]
use prost::{Message as _, Oneof};

#[derive(Clone, PartialEq, prost::Message)]
pub struct ContractIdentifier {
    #[prost(string, tag = "1")] pub exchange_code: String,
    #[prost(string, tag = "2")] pub contract_code: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Contract {
    #[prost(message, optional, tag = "1")] pub identifier: Option<ContractIdentifier>,
    #[prost(uint32, tag = "3")] pub dec_in_premium: u32,
    #[prost(int32, tag = "6")] pub r#type: i32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct PriceQty {
    #[prost(int64, tag = "1")] pub price: i64,
    #[prost(uint64, tag = "2")] pub qty: u64,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Header { #[prost(string, tag = "1")] pub request_id: String }
#[derive(Clone, PartialEq, prost::Message)]
pub struct Heartbeat {}
#[derive(Clone, PartialEq, prost::Message)]
pub struct LoginReq {
    #[prost(string, tag = "1")] pub username: String,
    #[prost(string, tag = "2")] pub password: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct LoginResp { #[prost(int32, tag = "1")] pub error: i32 } // 0 = ok
#[derive(Clone, PartialEq, prost::Message)]
pub struct GetContractsReq {
    #[prost(uint64, tag = "1")] pub offset: u64,
    #[prost(uint64, tag = "2")] pub limit: u64,
    #[prost(string, tag = "3")] pub exchange_code: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct GetContractsResp {
    #[prost(uint64, tag = "1")] pub offset: u64,
    #[prost(uint64, tag = "2")] pub total_count: u64,
    #[prost(message, repeated, tag = "3")] pub contracts: Vec<Contract>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Subscribe {
    #[prost(message, optional, tag = "1")] pub id: Option<ContractIdentifier>,
    #[prost(int32, tag = "2")] pub r#type: i32, // 1 = best bid/ask
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct SubStatus {
    #[prost(message, optional, tag = "1")] pub id: Option<ContractIdentifier>,
    #[prost(int32, tag = "2")] pub r#type: i32,
    #[prost(bool, tag = "3")] pub subscribed: bool,
    #[prost(int32, tag = "4")] pub error: i32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct BestBidAsk {
    #[prost(message, optional, tag = "1")] pub id: Option<ContractIdentifier>,
    #[prost(message, optional, tag = "2")] pub bid: Option<PriceQty>,
    #[prost(message, optional, tag = "3")] pub ask: Option<PriceQty>,
    #[prost(message, optional, tag = "4")] pub trade: Option<PriceQty>,
}

#[derive(Clone, PartialEq, Oneof)]
pub enum Payload {
    #[prost(message, tag = "2")] Heartbeat(Heartbeat),
    #[prost(message, tag = "3")] LoginReq(LoginReq),
    #[prost(message, tag = "4")] LoginResp(LoginResp),
    #[prost(message, tag = "7")] GetContractsReq(GetContractsReq),
    #[prost(message, tag = "8")] GetContractsResp(GetContractsResp),
    #[prost(message, tag = "9")] Subscribe(Subscribe),
    #[prost(message, tag = "11")] SubStatus(SubStatus),
    #[prost(message, tag = "12")] BestBidAsk(BestBidAsk),
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Message {
    #[prost(message, optional, tag = "1")] pub header: Option<Header>,
    #[prost(oneof = "Payload", tags = "2, 3, 4, 7, 8, 9, 11, 12")] pub payload: Option<Payload>,
}

pub fn encode(p: Payload) -> Vec<u8> {
    Message { header: None, payload: Some(p) }.encode_to_vec()
}
