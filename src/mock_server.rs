// Fake price adaptor, also a load generator: `N=1000 HZ=2 cargo run --release --example mock_server`
// Listens on :8898. Serves N contracts named C0..C{N-1} on any exchange code, replies to heartbeats,
// and pushes HZ best-bid/ask updates per second for every subscribed contract (N * HZ ticks/s in total).
#[path = "../src/pb.rs"] mod pb;
use futures_util::{SinkExt, StreamExt};
use pb::{Payload, *};
use prost::Message as _;
use std::time::Duration;
use tokio_tungstenite::{accept_async, tungstenite::Message as Ws};

fn var(k: &str, default: u64) -> u64 { std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(default) }

#[tokio::main]
async fn main() {
    let (n, hz) = (var("N", 2), var("HZ", 2));
    let l = tokio::net::TcpListener::bind("127.0.0.1:8898").await.unwrap();
    println!("mock on ws://127.0.0.1:8898, {n} contracts x {hz}/s");
    while let Ok((s, _)) = l.accept().await {
        tokio::spawn(async move {
            let (mut tx, mut rx) = accept_async(s).await.unwrap().split();
            let mut subs: Vec<ContractIdentifier> = vec![];
            let mut push = tokio::time::interval(Duration::from_micros(1_000_000 / hz));
            let mut px = 62600i64;
            loop {
                tokio::select! {
                    m = rx.next() => {
                        let Some(Ok(Ws::Binary(b))) = m else { return };
                        let reply = match pb::Message::decode(&b[..]).unwrap().payload {
                            Some(Payload::LoginReq(_)) => Some(Payload::LoginResp(LoginResp { error: 0 })),
                            Some(Payload::Heartbeat(h)) => Some(Payload::Heartbeat(h)),
                            Some(Payload::GetContractsReq(r)) => {
                                let page = (r.offset..n.min(r.offset + r.limit.clamp(1, 500))).map(|i| Contract {
                                    identifier: Some(ContractIdentifier { exchange_code: r.exchange_code.clone(), contract_code: format!("C{i}") }),
                                    dec_in_premium: 1, r#type: 1 }).collect();
                                Some(Payload::GetContractsResp(GetContractsResp { offset: r.offset, total_count: n, contracts: page }))
                            }
                            Some(Payload::Subscribe(s)) => { subs.push(s.id.clone().unwrap());
                                Some(Payload::SubStatus(SubStatus { id: s.id, r#type: 1, subscribed: true, error: 0 })) }
                            _ => None,
                        };
                        if let Some(p) = reply { if tx.send(Ws::Binary(encode(p))).await.is_err() { return } }
                    }
                    _ = push.tick() => for id in &subs {
                        px += 2;
                        let q = |p, q| Some(PriceQty { price: p, qty: q });
                        let u = BestBidAsk { id: Some(id.clone()), bid: q(px, 3), ask: q(px + 4, 5), trade: q(px + 2, 1) };
                        if tx.send(Ws::Binary(encode(Payload::BestBidAsk(u)))).await.is_err() { return }
                    }
                }
            }
        });
    }
}
