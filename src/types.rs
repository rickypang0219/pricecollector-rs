// Wire types for the Adapter price protocol (TCP port): little-endian, pack(1).
// Frame = int16 type, uint16 total length, then the body. Offsets below are relative to the body.
fn cstr(b: &[u8]) -> String {
    let n = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..n]).into_owned()
}
fn i16_at(b: &[u8], o: usize) -> i16 {
    i16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}
fn i32_at(b: &[u8], o: usize) -> i32 {
    i32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

#[derive(Debug)]
pub struct Contract {
    pub exchange: String,
    pub code: String,
    pub decimals: u32,
    pub kind: i16,
}

#[derive(Debug)]
pub enum Msg {
    LoginRet {
        session: String,
        err: i16,
    },
    Heartbeat,
    Exchanges {
        last: bool,
        codes: Vec<String>,
    },
    Contracts {
        last: bool,
        list: Vec<Contract>,
    },
    SubRet {
        err: i16,
    },
    Best {
        exchange: String,
        code: String,
        bid: (i32, u32),
        ask: (i32, u32),
        trade: (i32, u32),
    }, // (raw price, qty)
    Snap {
        exchange: String,
        code: String,
        open: i32,
        high: i32,
        low: i32,
        close: i32,
        volume: u64,
        oi: u64,
    },
    Disconnect {
        user: String,
    },
    Other {
        msg_type: i16,
        len: usize,
    },
}

pub fn decode(t: i16, b: &[u8]) -> Msg {
    // record count: trust the counter, but never past the array capacity or the bytes actually received
    let count = |cap: usize, size: usize| {
        (i16_at(b, 20).max(0) as usize)
            .min(cap)
            .min((b.len() - 22) / size)
    };
    let px = |o: usize| (i32_at(b, o + 1), u32_at(b, o + 21)); // LAPrice: nPremium at +1, nQty at +21
    match t {
        2 if b.len() >= 24 => Msg::LoginRet {
            session: cstr(&b[9..14]),
            err: i16_at(b, 14),
        },
        3 => Msg::Heartbeat,
        8 if b.len() >= 22 => Msg::Exchanges {
            last: i16_at(b, 18) != 0,
            codes: (0..count(10, 90))
                .map(|i| cstr(&b[22 + i * 90..52 + i * 90]))
                .collect(),
        },
        10 if b.len() >= 22 => Msg::Contracts {
            last: i16_at(b, 18) != 0,
            list: (0..count(5, 226))
                .map(|i| {
                    let r = &b[22 + i * 226..];
                    Contract {
                        exchange: cstr(&r[..30]),
                        code: cstr(&r[110..140]),
                        decimals: u32_at(r, 190),
                        kind: i16_at(r, 202),
                    }
                })
                .collect(),
        },
        12 if b.len() >= 24 => Msg::SubRet { err: i16_at(b, 22) },
        40 if b.len() >= 187 => Msg::Best {
            exchange: cstr(&b[..30]),
            code: cstr(&b[30..60]),
            bid: px(60),
            ask: px(85),
            trade: px(110),
        },
        10007 if b.len() >= 124 => Msg::Snap {
            exchange: cstr(&b[..30]),
            code: cstr(&b[30..60]),
            high: i32_at(b, 60),
            low: i32_at(b, 64),
            open: i32_at(b, 68),
            close: i32_at(b, 72),
            volume: u64_at(b, 108),
            oi: u64_at(b, 116),
        },
        26 if b.len() >= 78 => Msg::Disconnect {
            user: cstr(&b[14..78]),
        },
        _ => Msg::Other {
            msg_type: t,
            len: b.len() + 4,
        },
    }
}

fn put(f: &mut [u8], off: usize, max: usize, s: &str) {
    // fixed char[N]: zero-filled, always NUL-terminated
    let n = s.len().min(max);
    f[off..off + n].copy_from_slice(&s.as_bytes()[..n]);
}
fn frame(t: i16, body: usize, sys: &str, sess: &str) -> Vec<u8> {
    // header + the common SystemID/session prefix
    let mut f = vec![0u8; body + 4];
    f[..2].copy_from_slice(&t.to_le_bytes());
    f[2..4].copy_from_slice(&((body + 4) as u16).to_le_bytes());
    put(&mut f, 4, 8, sys);
    put(&mut f, 13, 4, sess);
    f
}
pub fn login(user: &str, pass: &str, sys: &str, sess: &str) -> Vec<u8> {
    let mut f = frame(1, 36, "", "");
    put(&mut f, 4, 8, user);
    put(&mut f, 13, 12, pass);
    put(&mut f, 26, 8, sys);
    put(&mut f, 35, 4, sess);
    f
}
pub fn heartbeat(sys: &str, sess: &str) -> Vec<u8> {
    frame(3, 18, sys, sess)
}
pub fn exchange_req(sys: &str, sess: &str) -> Vec<u8> {
    frame(7, 18, sys, sess)
}
pub fn contract_req(sys: &str, sess: &str, exchange: &str) -> Vec<u8> {
    let mut f = frame(9, 48, sys, sess);
    put(&mut f, 22, 29, exchange);
    f
}
// One subscribe frame carries up to 10 (exchange, code) slots of 60 bytes each; always the full 630 bytes.
pub fn subscribe(sys: &str, sess: &str, sub_type: i16, contracts: &[(String, String)]) -> Vec<u8> {
    let mut f = frame(11, 626, sys, sess); // nSub = 0 (subscribe)
    f[20..22].copy_from_slice(&sub_type.to_le_bytes());
    f[28..30].copy_from_slice(&(contracts.len() as i16).to_le_bytes()); // nContractNum
    for (i, (ex, code)) in contracts.iter().take(10).enumerate() {
        put(&mut f, 30 + i * 60, 29, ex);
        put(&mut f, 60 + i * 60, 29, code);
    }
    f
}
