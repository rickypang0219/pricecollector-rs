// Adapter TCP port -> QuestDB. Logs in, lists every exchange and contract, subscribes all of them,
// and writes every update to QuestDB. No protobuf, no WebSocket: plain TCP and fixed byte offsets (types.rs).
//
//   ADDR=192.x.x.x:5489 USER_ID=.. PASSWORD=.. SYSTEM_ID=SZ00000 QUESTDB="ws::addr=localhost:9000;" cargo run --release
//
// QUESTDB    questdb-rs config string. ws:: = QWP (QuestDB 10+). http::addr=localhost:9000; = ILP for older servers.
//            Unset = no DB, just the stats line.
// KINDS      comma-separated contract types to subscribe, e.g. KINDS=1 for futures only. Unset = everything.
// FLUSH_MS   how long the DB writer collects rows before one flush (default 100)
// PRINT=1    print every update
// RAW=1      print every msg 40 / 10007 frame as hex, marked SAME (byte-identical to the previous one) or diff
mod types;
use questdb::ingress::{Buffer, Sender, TimestampNanos};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::{
    collections::HashMap,
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{tcp::OwnedReadHalf, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use types::*;

type Rd = BufReader<OwnedReadHalf>;
type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

static IN: AtomicU64 = AtomicU64::new(0); // updates read from the socket
static DB: AtomicU64 = AtomicU64::new(0); // rows handed to QuestDB
static LOST: AtomicU64 = AtomicU64::new(0); // rows the DB writer did not deliver

#[derive(Clone, Debug)]
pub enum Row {
    // what goes on the bus: prices already divided by 10^decimals
    Bbo {
        ns: i64,
        exchange: String,
        code: String,
        bid: f64,
        bid_qty: u32,
        ask: f64,
        ask_qty: u32,
        last: f64,
        last_qty: u32,
    },
    Snap {
        ns: i64,
        exchange: String,
        code: String,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: u64,
        oi: u64,
    },
}

// One frame = 4-byte header + (nLen - 4) body bytes. read_exact waits for all of them, so no manual buffer.
async fn read_msg(r: &mut Rd) -> Res<Msg> {
    let mut h = [0u8; 4];
    r.read_exact(&mut h).await?;
    let (t, len) = (
        i16::from_le_bytes([h[0], h[1]]),
        u16::from_le_bytes([h[2], h[3]]) as usize,
    );
    if len < 4 {
        return Err(format!("bad frame length {len}").into());
    }
    let mut body = vec![0u8; len - 4];
    r.read_exact(&mut body).await?;
    if t == 40 || t == 10007 {
        check_dup(t, &body);
    }
    Ok(decode(t, &body))
}

// Duplicate check: compare each market-data frame with the previous frame of the same type and contract.
// SAME = byte-identical to the previous one (a true duplicate). RAW=1 prints every frame as hex.
static SAME: AtomicU64 = AtomicU64::new(0);
static DIFF: AtomicU64 = AtomicU64::new(0);
fn check_dup(t: i16, body: &[u8]) {
    use std::sync::{Mutex, OnceLock};
    static LAST: OnceLock<Mutex<HashMap<(i16, Vec<u8>), (Vec<u8>, std::time::Instant)>>> =
        OnceLock::new();
    let key = (t, body[..60.min(body.len())].to_vec()); // message type + exchange + contract code
    let now = std::time::Instant::now();
    let mut last = LAST.get_or_init(Default::default).lock().unwrap();
    let verdict = match last.get(&key) {
        Some((prev, at)) if prev == body => {
            SAME.fetch_add(1, Relaxed);
            format!("SAME +{:.3}ms", (now - *at).as_secs_f64() * 1e3)
        }
        Some((prev, at)) => {
            DIFF.fetch_add(1, Relaxed);
            let bytes: Vec<usize> = (0..body.len())
                .filter(|&i| prev.get(i) != Some(&body[i]))
                .collect();
            format!(
                "diff +{:.3}ms at body bytes {:?}",
                (now - *at).as_secs_f64() * 1e3,
                bytes
            )
        }
        None => "first".into(),
    };
    if env::var("RAW").is_ok() {
        let hex: String = body.iter().map(|b| format!("{b:02x}")).collect();
        let code = String::from_utf8_lossy(&body[30..60.min(body.len())])
            .trim_end_matches('\0')
            .to_string();
        println!("{t} {code} {verdict}\n  {hex}");
    }
    last.insert(key, (body.to_vec(), now));
}

// ---------------------------------------------------------------- producer
async fn session(
    addr: &str,
    user: &str,
    pass: &str,
    sys: &str,
    bus: &broadcast::Sender<Row>,
) -> Res<()> {
    let (rd, mut wr) = TcpStream::connect(addr).await?.into_split();
    let mut r = BufReader::new(rd);
    // every outgoing frame goes through this channel, so requests and heartbeats never interleave mid-frame
    let (out, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let writer = tokio::spawn(async move {
        while let Some(f) = out_rx.recv().await {
            if wr.write_all(&f).await.is_err() {
                break;
            }
        }
    });

    out.send(login(user, pass, sys, "0001"))?;
    let sess = loop {
        if let Msg::LoginRet { session, err } = read_msg(&mut r).await? {
            if err != 0 {
                return Err(format!("login failed, nErrorID={err}").into());
            }
            break session;
        }
    };
    println!("logged in to {addr}, session={sess}");

    // heartbeat from here on, including during the contract download
    let (hb_out, hb_sys, hb_sess) = (out.clone(), sys.to_string(), sess.clone());
    let hb = tokio::spawn(async move {
        let mut tick = time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            if hb_out.send(heartbeat(&hb_sys, &hb_sess)).is_err() {
                break;
            }
        }
    });

    let result = async {
        out.send(exchange_req(sys, &sess))?;
        let mut exchanges = vec![];
        loop {
            if let Msg::Exchanges { last, codes } = read_msg(&mut r).await? {
                exchanges.extend(codes);
                if last {
                    break;
                }
            }
        }

        let kinds: Option<Vec<i16>> = env::var("KINDS")
            .ok()
            .map(|s| s.split(',').filter_map(|k| k.trim().parse().ok()).collect());
        let mut decimals: HashMap<(String, String), i32> = HashMap::new();
        for ex in &exchanges {
            out.send(contract_req(sys, &sess, ex))?;
            let mut n = 0;
            loop {
                if let Msg::Contracts { last, list } = read_msg(&mut r).await? {
                    for c in list {
                        if kinds.as_ref().map_or(true, |k| k.contains(&c.kind)) {
                            decimals.insert((c.exchange, c.code), c.decimals as i32);
                            n += 1
                        }
                    }
                    if last {
                        break;
                    }
                }
            }
            println!("{ex}: {n} contracts");
        }
        let all: Vec<(String, String)> = decimals.keys().cloned().collect();
        for batch in all.chunks(10) {
            out.send(subscribe(sys, &sess, 0, batch))?;
        }
        println!(
            "subscribe sent for {} contracts on {} exchanges",
            all.len(),
            exchanges.len()
        );

        let (mut last_hb, mut seen) = (std::time::Instant::now(), std::collections::HashSet::new());
        loop {
            let m = match time::timeout(Duration::from_secs(30), read_msg(&mut r)).await {
                Ok(Ok(m)) => m,
                Ok(Err(e)) => {
                    return Err(format!(
                        "{e} (last server heartbeat {:.1}s ago)",
                        last_hb.elapsed().as_secs_f64()
                    )
                    .into())
                }
                Err(_) => return Err("nothing received for 30s".into()),
            };
            let ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64;
            let scale = |ex: &str, code: &str| {
                decimals
                    .get(&(ex.to_string(), code.to_string()))
                    .map(|&d| 10f64.powi(d))
            };
            let short = |ex: &str| ex.split('-').next().unwrap_or(ex).to_string(); // CFFEX-00816596-PD -> CFFEX
            let row = match m {
                Msg::Best {
                    exchange,
                    code,
                    bid,
                    ask,
                    trade,
                } => {
                    let Some(s) = scale(&exchange, &code) else {
                        continue;
                    };
                    Row::Bbo {
                        ns,
                        bid: bid.0 as f64 / s,
                        bid_qty: bid.1,
                        ask: ask.0 as f64 / s,
                        ask_qty: ask.1,
                        last: trade.0 as f64 / s,
                        last_qty: trade.1,
                        exchange: short(&exchange),
                        code,
                    }
                }
                Msg::Snap {
                    exchange,
                    code,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    oi,
                } => {
                    let Some(s) = scale(&exchange, &code) else {
                        continue;
                    };
                    Row::Snap {
                        ns,
                        open: open as f64 / s,
                        high: high as f64 / s,
                        low: low as f64 / s,
                        close: close as f64 / s,
                        volume,
                        oi,
                        exchange: short(&exchange),
                        code,
                    }
                }
                Msg::SubRet { err } if err != 0 => {
                    eprintln!("subscribe rejected, error={err}");
                    continue;
                }
                Msg::Disconnect { user } => {
                    return Err(format!("server sent disconnect (msg 26), user={user}").into())
                }
                Msg::Heartbeat => {
                    last_hb = std::time::Instant::now();
                    continue;
                }
                Msg::Other { msg_type, len } => {
                    if seen.insert(msg_type) {
                        println!("first msg {msg_type} ({len} bytes), not decoded")
                    }
                    continue;
                }
                _ => continue,
            };
            IN.fetch_add(1, Relaxed);
            let _ = bus.send(row); // never waits; Err only means no consumer is listening
        }
    }
    .await;
    hb.abort();
    writer.abort();
    result
}

// ---------------------------------------------------------------- consumers
fn add(b: &mut Buffer, row: &Row) -> questdb::Result<()> {
    match row {
        Row::Bbo {
            ns,
            exchange,
            code,
            bid,
            bid_qty,
            ask,
            ask_qty,
            last,
            last_qty,
        } => b
            .table("bbo")?
            .symbol("symbol", code)?
            .symbol("exchange", exchange)?
            .column_f64("bid_px", *bid)?
            .column_i64("bid_sz", *bid_qty as i64)?
            .column_f64("ask_px", *ask)?
            .column_i64("ask_sz", *ask_qty as i64)?
            .column_f64("last_trade_px", *last)?
            .column_i64("last_sz", *last_qty as i64)?
            .at(TimestampNanos::new(*ns)),
        Row::Snap {
            ns,
            exchange,
            code,
            open,
            high,
            low,
            close,
            volume,
            oi,
        } => b
            .table("snap")?
            .symbol("symbol", code)?
            .symbol("exchange", exchange)?
            .column_f64("open", *open)?
            .column_f64("high", *high)?
            .column_f64("low", *low)?
            .column_f64("close", *close)?
            .column_i64("volume", *volume as i64)?
            .column_i64("oi", *oi as i64)?
            .at(TimestampNanos::new(*ns)),
    }
}

fn put(buf: &mut Buffer, r: &Row) -> u64 {
    match add(buf, r) {
        Ok(()) => 1,
        Err(e) => {
            LOST.fetch_add(1, Relaxed);
            eprintln!("questdb: bad row skipped: {e}");
            0
        }
    }
}

// Tables are created on startup if missing (CREATE TABLE IF NOT EXISTS), via QuestDB's HTTP /exec on the same host:port.
const DDL: [&str; 2] = [
    "CREATE TABLE IF NOT EXISTS bbo (recv_ts TIMESTAMP, symbol SYMBOL, exchange SYMBOL, \
     bid_px DOUBLE, bid_sz LONG, ask_px DOUBLE, ask_sz LONG, last_trade_px DOUBLE, last_sz LONG) \
     TIMESTAMP(recv_ts) PARTITION BY DAY WAL",
    "CREATE TABLE IF NOT EXISTS snap (recv_ts TIMESTAMP, symbol SYMBOL, exchange SYMBOL, \
     open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume LONG, oi LONG) \
     TIMESTAMP(recv_ts) PARTITION BY DAY WAL",
];
fn ensure_tables(conf: &str) -> Res<()> {
    let addr = conf
        .split(';')
        .find_map(|kv| kv.split("::").last()?.strip_prefix("addr="))
        .ok_or("no addr= in QUESTDB")?;
    for sql in DDL {
        ureq::get(&format!("http://{addr}/exec"))
            .query("query", sql)
            .call()
            .map_err(|e| format!("create table failed: {e}"))?;
    }
    println!("questdb: tables bbo, snap ready");
    Ok(())
}

// QuestDB writer on its own OS thread (the client is blocking, and this keeps DB IO off the socket reader).
// Tables are created by ensure_tables() if missing. Designated timestamp = recv_ts (our clock).
fn questdb_writer(conf: String, mut rx: broadcast::Receiver<Row>) {
    use broadcast::error::{RecvError, TryRecvError};
    let flush_ms = env::var("FLUSH_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    loop {
        // (re)connect
        if let Err(e) = ensure_tables(&conf) {
            eprintln!("questdb: {e}");
            std::thread::sleep(Duration::from_secs(2));
            continue;
        }
        let mut sender = match Sender::from_conf(&conf) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("questdb: connect failed: {e}");
                std::thread::sleep(Duration::from_secs(2));
                continue;
            }
        };
        println!("questdb: connected with {conf}");
        let mut buf = sender.new_buffer();
        loop {
            let first = match rx.blocking_recv() {
                Ok(r) => r,
                Err(RecvError::Lagged(k)) => {
                    LOST.fetch_add(k, Relaxed);
                    continue;
                }
                Err(RecvError::Closed) => return,
            };
            std::thread::sleep(Duration::from_millis(flush_ms)); // let a batch build up
            let mut n = put(&mut buf, &first);
            while n < 100_000 {
                match rx.try_recv() {
                    // take everything queued: a slow DB gets bigger batches, not a backlog
                    Ok(r) => n += put(&mut buf, &r),
                    Err(TryRecvError::Lagged(k)) => {
                        LOST.fetch_add(k, Relaxed);
                    }
                    Err(_) => break,
                }
            }
            match sender.flush(&mut buf) {
                Ok(()) => {
                    DB.fetch_add(n, Relaxed);
                }
                Err(e) => {
                    LOST.fetch_add(n, Relaxed);
                    eprintln!("questdb: {n} rows dropped: {e}");
                    buf.clear();
                    if sender.must_close() {
                        break;
                    } else {
                        std::thread::sleep(Duration::from_secs(1))
                    }
                }
            }
        }
    }
}

async fn printer(mut rx: broadcast::Receiver<Row>) {
    loop {
        match rx.recv().await {
            Ok(r) => println!("{r:?}"),
            Err(broadcast::error::RecvError::Closed) => return,
            _ => {}
        }
    }
}

// one line every 10s: the record of the run. `in` and `db` should match; `lost` should stay 0.
async fn stats() {
    let mut every = time::interval(Duration::from_secs(10));
    every.tick().await;
    let (mut in0, mut db0) = (0, 0);
    loop {
        every.tick().await;
        let (i, d) = (IN.load(Relaxed), DB.load(Relaxed));
        let s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
            % 86_400;
        println!(
            "{:02}:{:02}:{:02}Z stats in={:.1}/s db={:.1}/s lost={} same={} diff={}",
            s / 3600,
            s % 3600 / 60,
            s % 60,
            (i - in0) as f64 / 10.0,
            (d - db0) as f64 / 10.0,
            LOST.load(Relaxed),
            SAME.load(Relaxed),
            DIFF.load(Relaxed)
        );
        (in0, db0) = (i, d);
    }
}

// ---------------------------------------------------------------- wiring
#[tokio::main]
async fn main() -> Res<()> {
    let var = |k: &str| env::var(k).unwrap_or_default();
    let (addr, user, pass, sys) = (
        var("ADDR"),
        var("USER_ID"),
        var("PASSWORD"),
        var("SYSTEM_ID"),
    );
    if addr.is_empty() {
        return Err("set ADDR=host:port (plus USER_ID, PASSWORD, SYSTEM_ID)".into());
    }

    let (bus, _) = broadcast::channel::<Row>(65_536);
    tokio::spawn(stats());
    if env::var("PRINT").is_ok() {
        tokio::spawn(printer(bus.subscribe()));
    }
    if let Ok(conf) = env::var("QUESTDB") {
        let rx = bus.subscribe();
        std::thread::spawn(move || questdb_writer(conf, rx));
    }
    // more consumers (z-score, Kafka, ...) = one more `tokio::spawn(f(bus.subscribe()))`

    loop {
        if let Err(e) = session(&addr, &user, &pass, &sys, &bus).await {
            eprintln!("disconnected: {e}")
        }
        time::sleep(Duration::from_secs(2)).await; // reconnect: login, list and subscribe from scratch
    }
}
