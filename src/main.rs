// botRD — Discord forum gatekeeper. Single-threaded async Rust, tuned for a small memory footprint:
// gateway events are parsed straight into the few fields we need (no generic JSON tree), and
// everything runs on one thread with one shared TLS configuration.
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::client::conn::http1::{self, SendRequest};
use hyper::header::{HeaderValue, AUTHORIZATION, CONTENT_TYPE, HOST, USER_AGENT};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::{spawn_local, JoinHandle};
use tokio::time::{sleep, timeout};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use unicode_properties::{GeneralCategoryGroup as Cat, UnicodeGeneralCategory};

// ─── Errors ───────────────────────────────────────────────────────────────────
#[derive(Debug)]
struct Error { status: u16, msg: String } // status 0 = not an HTTP error
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { f.write_str(&self.msg) }
}
impl From<std::io::Error> for Error { fn from(e: std::io::Error) -> Self { Error { status: 0, msg: e.to_string() } } }
impl From<serde_json::Error> for Error { fn from(e: serde_json::Error) -> Self { Error { status: 0, msg: e.to_string() } } }
type Res<T> = Result<T, Error>;

// ─── Storage (same data.json layout as the Node version) ──────────────────────
#[derive(Serialize, Deserialize, Default)]
struct Db {
    #[serde(default)] guilds: HashMap<String, Guild>,
    #[serde(default, rename = "commandsHash")] commands_hash: Option<String>,
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Guild {
    #[serde(default)] forum_id: Option<String>,
    #[serde(default)] role_id: Option<String>,
    #[serde(default)] phase: u8, // 0 = closed / not open
    #[serde(default)] lists: Lists,
}

#[derive(Serialize, Deserialize, Default)]
struct Lists {
    #[serde(default, rename = "1")] one: List,
    #[serde(default, rename = "2")] two: List,
}
impl Lists {
    fn get(&self, n: u8) -> &List { if n == 2 { &self.two } else { &self.one } }
    fn get_mut(&mut self, n: u8) -> &mut List { if n == 2 { &mut self.two } else { &mut self.one } }
}

#[derive(Serialize, Deserialize, Default)]
struct List {
    #[serde(default)] words: Vec<String>,
    #[serde(default)] tags: Vec<Tag>,
    #[serde(skip)] compiled: RefCell<Option<Rc<Compiled>>>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct Tag { id: String, #[serde(default)] name: String }

fn load_db(path: &str) -> Db {
    let Ok(bytes) = std::fs::read(path) else { return Db::default() };
    let raw: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => { eprintln!("{path} is not valid JSON ({e}), starting with empty settings"); return Db::default(); }
    };
    let has_guilds = raw.get("guilds").is_some_and(|g| !g.is_null());
    let raw = if has_guilds { raw } else { json!({ "guilds": raw }) }; // migrate v1 layout
    serde_json::from_value(raw).unwrap_or_else(|e| { eprintln!("{path} has an unexpected shape ({e}), starting with empty settings"); Db::default() })
}

// ─── Blocklist matching (compiled once per list, O(text) per check) ───────────
// Plain words/phrases → token-set lookup: whole-word, case-insensitive, any separator
// ("free nitro" catches "Free-Nitro"). Entries containing symbols/emoji → substring match.
struct Compiled { tags: HashSet<String>, phrases: HashMap<String, String>, max_n: usize, raw: Vec<String> }
impl Compiled { fn has_words(&self) -> bool { !self.phrases.is_empty() || !self.raw.is_empty() } }

fn is_word(c: char) -> bool {
    if c.is_ascii() { c.is_ascii_alphanumeric() || c == '_' } else { matches!(c.general_category_group(), Cat::Letter | Cat::Number) }
}
fn is_plain(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_whitespace() || c == '\u{feff}' || matches!(c.general_category_group(), Cat::Letter | Cat::Number | Cat::Punctuation))
}
fn tokens(lower: &str) -> impl Iterator<Item = &str> { lower.split(|c| !is_word(c)).filter(|t| !t.is_empty()) }

fn compile(list: &List) -> Rc<Compiled> {
    if let Some(c) = list.compiled.borrow().as_ref() { return c.clone(); }
    let mut c = Compiled { tags: list.tags.iter().map(|t| t.id.clone()).collect(), phrases: HashMap::new(), max_n: 1, raw: Vec::new() };
    for w in &list.words {
        let lower = w.to_lowercase();
        if is_plain(w) {
            let t: Vec<&str> = tokens(&lower).collect();
            if !t.is_empty() { c.max_n = c.max_n.max(t.len()); c.phrases.insert(t.join(" "), w.clone()); }
        } else { c.raw.push(lower); }
    }
    let c = Rc::new(c);
    *list.compiled.borrow_mut() = Some(c.clone());
    c
}

fn find_word(text: &str, l: &Compiled) -> Option<String> {
    if text.is_empty() || !l.has_words() { return None; }
    let lower = text.to_lowercase();
    if let Some(r) = l.raw.iter().find(|r| lower.contains(r.as_str())) { return Some(r.clone()); }
    if l.phrases.is_empty() { return None; }
    let t: Vec<&str> = tokens(&lower).collect();
    let mut gram = String::new();
    for i in 0..t.len() {
        gram.clear(); gram.push_str(t[i]);
        let mut hit = l.phrases.get(gram.as_str());
        let mut n = 1;
        while hit.is_none() && n < l.max_n && i + n < t.len() {
            gram.push(' '); gram.push_str(t[i + n]);
            hit = l.phrases.get(gram.as_str());
            n += 1;
        }
        if let Some(h) = hit { return Some(h.clone()); }
    }
    None
}

// ─── Small helpers ────────────────────────────────────────────────────────────
fn rand01() -> f64 { // jitter only, so the clock's sub-second noise is random enough
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0.5, |d| (d.subsec_nanos() % 1_000_000) as f64 / 1e6)
}
fn pct(s: &str) -> String { // encodeURIComponent
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) { out.push(b as char); } else { out.push_str(&format!("%{b:02X}")); }
    }
    out
}
/// Returns the prefix of `s` that fits in `n` UTF-16 units (how Discord counts), or None if all of `s` fits.
fn truncated(s: &str, n: usize) -> Option<&str> {
    let mut units = 0;
    for (i, c) in s.char_indices() {
        units += c.len_utf16();
        if units > n { return Some(&s[..i]); }
    }
    None
}
fn fnv1a(s: &str) -> u64 { s.bytes().fold(0xcbf29ce484222325, |h, b| (h ^ b as u64).wrapping_mul(0x100000001b3)) }

/// Values from the process environment, falling back to a `.env` file in the working directory.
fn env_lookup() -> impl Fn(&str) -> Option<String> {
    let file = std::fs::read_to_string(".env").unwrap_or_default();
    move |key| {
        if let Ok(v) = std::env::var(key) { return Some(v); }
        file.lines().find_map(|line| {
            let line = line.trim();
            let (k, v) = line.strip_prefix("export ").unwrap_or(line).split_once('=')?;
            if k.trim() != key { return None; }
            let v = v.trim();
            let unquoted = v.strip_prefix('"').and_then(|x| x.strip_suffix('"')).or_else(|| v.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')));
            Some(unquoted.unwrap_or(v).to_owned())
        })
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn release_free_memory() { extern "C" { fn malloc_trim(pad: usize) -> i32; } unsafe { malloc_trim(0); } }
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn release_free_memory() {}

// ─── Gateway payloads: only the fields we read ────────────────────────────────
#[derive(Deserialize)]
struct Envelope<'a> { op: u8, #[serde(borrow, default)] d: Option<&'a RawValue>, #[serde(default)] s: Option<u64>, #[serde(default)] t: Option<&'a str> }
#[derive(Deserialize)]
struct Hello { heartbeat_interval: u64 }
#[derive(Deserialize)]
struct Ready { user: User, session_id: String, resume_gateway_url: String, application: Id }
#[derive(Deserialize)]
struct User { #[serde(default)] id: String, #[serde(default)] username: String }
#[derive(Deserialize)]
struct Id { id: String }
#[derive(Deserialize)]
struct Msg<'a> { id: &'a str, channel_id: &'a str, #[serde(default)] guild_id: Option<&'a str>, #[serde(borrow, default)] content: Option<&'a RawValue> }
#[derive(Deserialize)]
struct Content { #[serde(default)] content: Option<String> }
#[derive(Deserialize)]
struct Thread {
    id: String,
    #[serde(default)] guild_id: Option<String>,
    #[serde(default)] parent_id: Option<String>,
    #[serde(default)] owner_id: Option<String>,
    #[serde(default)] name: Option<String>,
    #[serde(default)] newly_created: Option<bool>,
    #[serde(default)] applied_tags: Option<Vec<String>>,
}
#[derive(Deserialize)]
struct GuildCreate<'a> { #[serde(borrow, default)] channels: Vec<Channel<'a>> }
#[derive(Deserialize)]
struct Channel<'a> { // tags/overwrites stay unparsed unless the channel is a forum
    id: &'a str,
    #[serde(default, rename = "type")] kind: Option<u8>,
    #[serde(borrow, default)] available_tags: Option<&'a RawValue>,
    #[serde(borrow, default)] permission_overwrites: Option<&'a RawValue>,
}
#[derive(Deserialize, Default)]
struct Overwrite { id: String, #[serde(default)] allow: String, #[serde(default)] deny: String }
#[derive(Deserialize)]
struct Overwrites { #[serde(default)] permission_overwrites: Option<Vec<Overwrite>> }
#[derive(Deserialize)]
struct Interaction {
    id: String, token: String,
    #[serde(rename = "type")] kind: u8,
    #[serde(default)] guild_id: Option<String>,
    #[serde(default)] data: Option<CommandData>,
    #[serde(default)] member: Option<Member>,
}
#[derive(Deserialize)]
struct Member { #[serde(default)] user: Option<User> }
#[derive(Deserialize)]
struct CommandData { #[serde(default)] name: String, #[serde(default)] options: Option<Vec<CommandOption>> }
#[derive(Deserialize)]
struct CommandOption {
    name: String,
    #[serde(rename = "type")] kind: u8,
    #[serde(default)] value: Option<Value>,
    #[serde(default)] options: Option<Vec<CommandOption>>,
    #[serde(default)] focused: Option<bool>,
}

struct Forum { tags: Vec<Tag>, overwrites: Vec<Overwrite> }

/// A slash command invocation flattened to { name, subcommand, options }.
struct Parsed { name: String, sub: Option<String>, opts: Vec<(String, Value)>, focused: Option<Value> }
impl Parsed {
    fn new(data: CommandData) -> Self {
        let mut opts = data.options.unwrap_or_default();
        let mut sub = None;
        if opts.first().is_some_and(|o| o.kind == 1) {
            let first = opts.swap_remove(0);
            sub = Some(first.name);
            opts = first.options.unwrap_or_default();
        }
        let focused = opts.iter().find(|o| o.focused == Some(true)).map(|o| o.value.clone().unwrap_or(Value::Null));
        Parsed { name: data.name, sub, opts: opts.into_iter().map(|o| (o.name, o.value.unwrap_or(Value::Null))).collect(), focused }
    }
    fn str(&self, key: &str) -> &str { self.opts.iter().find(|(k, _)| k == key).and_then(|(_, v)| v.as_str()).unwrap_or("") }
    fn phase(&self) -> Option<u8> {
        self.opts.iter().find(|(k, _)| k == "phase").and_then(|(_, v)| v.as_u64()).filter(|n| *n == 1 || *n == 2).map(|n| n as u8)
    }
}

// ─── Permissions ──────────────────────────────────────────────────────────────
const READ: u128 = 1 << 10 | 1 << 16;          // View Channel | Read Message History
const WRITE: u128 = 1 << 11 | 1 << 38 | 1 << 6; // Send (= "Create Posts" in forums) | Send in Threads | Add Reactions

// ─── Slash commands ───────────────────────────────────────────────────────────
fn commands_json() -> String {
    let phase = json!({ "type": 4, "name": "phase", "description": "Which phase", "required": true, "choices": [{ "name": "Phase 1", "value": 1 }, { "name": "Phase 2", "value": 2 }] });
    let kind = json!({ "type": 3, "name": "type", "description": "Block a word or a forum tag", "required": true, "choices": [{ "name": "word", "value": "word" }, { "name": "tag", "value": "tag" }] });
    let value = json!({ "type": 3, "name": "value", "description": "Word(s) (comma-separated) or tag name", "required": true, "autocomplete": true });
    let mut commands = json!([
        { "name": "setup", "description": "Choose the forum to manage and the default members role", "options": [
            { "type": 7, "name": "forum", "description": "Forum channel to manage", "required": true, "channel_types": [15] },
            { "type": 8, "name": "role", "description": "Default members role", "required": true },
        ] },
        { "name": "open", "description": "Open the forum to members in the given phase", "options": [phase] },
        { "name": "close", "description": "Make the forum read-only for members" },
        { "name": "status", "description": "Show the forum settings and both blocklists" },
        { "name": "blocklist", "description": "Manage the per-phase blocklists", "options": [
            { "type": 1, "name": "add", "description": "Block words or a tag in a phase", "options": [phase, kind, value] },
            { "type": 1, "name": "remove", "description": "Unblock words or a tag in a phase", "options": [phase, kind, value] },
        ] },
    ]);
    for cmd in commands.as_array_mut().into_iter().flatten() {
        cmd["default_member_permissions"] = json!("32"); // Manage Server
        cmd["contexts"] = json!([0]);                     // guild only
    }
    commands.to_string()
}

// ─── The bot ──────────────────────────────────────────────────────────────────
const UA: &str = "DiscordBot (https://github.com, 2.0)";
// GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT. The latter two deliver starter messages over the gateway.
const INTENTS: u32 = 1 << 0 | 1 << 9 | 1 << 15;
const MAX_IDLE_CONNECTIONS: usize = 4;

struct Bot {
    token: String,
    auth: HeaderValue,
    data_file: String,
    tls: TlsConnector,
    db: RefCell<Db>,
    pool: RefCell<Vec<SendRequest<Full<Bytes>>>>,       // idle keep-alive REST connections, most recent last
    forums: RefCell<HashMap<String, Forum>>,            // forum id → tags + overwrites (fed by gateway events)
    dms: RefCell<VecDeque<(u64, u64)>>,                 // user id → DM channel id (FIFO-capped)
    early: RefCell<HashMap<String, (Instant, String)>>, // thread id → starter content (message arrived first)
    waiting: RefCell<HashMap<String, oneshot::Sender<String>>>, // thread id → resolver (thread arrived first)
    warm: RefCell<Option<JoinHandle<()>>>,
    me: RefCell<String>,
    seq: Cell<Option<u64>>,
    session: RefCell<Option<(String, String)>>,         // (session id, resume url)
}

impl Bot {
    fn new(token: String, auth: HeaderValue, data_file: String) -> Rc<Self> {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions().expect("TLS versions")
            .with_root_certificates(roots).with_no_client_auth();
        tls.resumption = rustls::client::Resumption::in_memory_sessions(8);

        Rc::new(Bot {
            token, auth,
            db: RefCell::new(load_db(&data_file)),
            data_file,
            tls: TlsConnector::from(Arc::new(tls)),
            pool: RefCell::default(), forums: RefCell::default(), dms: RefCell::default(), early: RefCell::default(),
            waiting: RefCell::default(), warm: RefCell::default(), me: RefCell::default(), seq: Cell::new(None), session: RefCell::default(),
        })
    }

    fn save(&self) -> Res<()> {
        let tmp = format!("{}.tmp", self.data_file);
        std::fs::write(&tmp, serde_json::to_vec(&*self.db.borrow())?)?;
        std::fs::rename(&tmp, &self.data_file)?;
        Ok(())
    }

    async fn tls_connect(&self, host: &str) -> std::io::Result<TlsStream<TcpStream>> {
        let tcp = TcpStream::connect((host, 443)).await?;
        tcp.set_nodelay(true)?;
        let _ = socket2::SockRef::from(&tcp).set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(Duration::from_secs(15)));
        let name = rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        self.tls.connect(name, tcp).await
    }

    // ─── REST: HTTP/1.1 over a small pool of keep-alive connections ───────────
    // Idle connections stay open until Discord closes them, so a deletion after a quiet
    // spell doesn't pay for a fresh TCP+TLS handshake.
    /// One attempt. On failure, the bool says whether it failed while sending on a reused connection.
    async fn request(&self, method: &Method, path: &str, body: Option<&Bytes>, reason: Option<&str>) -> Result<(u16, Bytes), (bool, String)> {
        let attempt = async {
            let mut reused = None;
            loop { // newest idle connection that is still alive
                let Some(mut tx) = self.pool.borrow_mut().pop() else { break };
                if tx.ready().await.is_ok() { reused = Some(tx); break; }
            }
            let was_reused = reused.is_some();
            let mut tx = match reused {
                Some(tx) => tx,
                None => {
                    let io = self.tls_connect("discord.com").await.map_err(|e| (false, e.to_string()))?;
                    let (tx, conn) = http1::handshake(TokioIo::new(io)).await.map_err(|e| (false, e.to_string()))?;
                    spawn_local(async move { let _ = conn.await; });
                    tx
                }
            };
            let mut req = Request::builder().method(method).uri(format!("/api/v10{path}"))
                .header(HOST, "discord.com").header(AUTHORIZATION, &self.auth).header(USER_AGENT, UA);
            if body.is_some() { req = req.header(CONTENT_TYPE, "application/json"); }
            if let Some(r) = reason { req = req.header("x-audit-log-reason", pct(r)); }
            let req = req.body(Full::new(body.cloned().unwrap_or_default())).map_err(|e| (false, e.to_string()))?;
            let res = tx.send_request(req).await.map_err(|e| (was_reused, e.to_string()))?;
            let status = res.status().as_u16();
            let bytes = res.into_body().collect().await.map_err(|e| (false, e.to_string()))?.to_bytes();
            let mut pool = self.pool.borrow_mut();
            pool.retain(|c| !c.is_closed());
            if pool.len() < MAX_IDLE_CONNECTIONS { pool.push(tx); }
            Ok((status, bytes))
        };
        timeout(Duration::from_secs(10), attempt).await.unwrap_or_else(|_| Err((false, "timeout".into())))
    }

    async fn api(&self, method: Method, path: &str, body: Option<String>, reason: Option<&str>) -> Res<Bytes> {
        let body = body.map(Bytes::from);
        let mut attempt = 0;
        loop {
            let (status, bytes) = match self.request(&method, path, body.as_ref(), reason).await {
                Ok(r) => r,
                // server closed a pooled connection just as we reused it — safe to resend
                Err((true, _)) if attempt < 2 => { attempt += 1; continue; }
                Err((_, msg)) => return Err(Error { status: 0, msg: format!("{method} {path} → {msg}") }),
            };
            if status == 429 && attempt < 3 {
                let wait = serde_json::from_slice::<Value>(&bytes).ok().and_then(|v| v["retry_after"].as_f64()).unwrap_or(1.0);
                sleep(Duration::from_secs_f64(wait.clamp(0.0, 600.0) + 0.05)).await;
                attempt += 1;
                continue;
            }
            if status >= 400 { return Err(Error { status, msg: format!("{method} {path} → {status} {}", String::from_utf8_lossy(&bytes)) }); }
            return Ok(bytes);
        }
    }

    /// While any forum is open, keep one TLS connection warm so a deletion goes out right away.
    fn update_warm(self: &Rc<Self>) {
        let open = self.db.borrow().guilds.values().any(|g| g.phase != 0);
        let mut warm = self.warm.borrow_mut();
        if open && warm.is_none() {
            let bot = self.clone();
            *warm = Some(spawn_local(async move {
                loop {
                    sleep(Duration::from_secs(30)).await;
                    let _ = bot.request(&Method::GET, "/gateway", None, None).await;
                }
            }));
        } else if !open {
            if let Some(task) = warm.take() { task.abort(); }
        }
    }

    // ─── Lookups ──────────────────────────────────────────────────────────────
    fn tag_name(&self, g: &Guild, id: &str) -> String {
        let forums = self.forums.borrow();
        let live = g.forum_id.as_ref().and_then(|f| forums.get(f)).and_then(|f| f.tags.iter().find(|t| t.id == id));
        live.or_else(|| g.lists.one.tags.iter().chain(&g.lists.two.tags).find(|t| t.id == id)).map_or_else(|| id.to_owned(), |t| t.name.clone())
    }

    fn describe_list(&self, g: &Guild, n: u8) -> String {
        let list = g.lists.get(n);
        let join = |items: Vec<String>| if items.is_empty() { "_none_".to_owned() } else { items.join(", ") };
        format!("**Phase {n}** — words: {} · tags: {}",
            join(list.words.iter().map(|w| format!("`{w}`")).collect()),
            join(list.tags.iter().map(|t| format!("`{}`", self.tag_name(g, &t.id))).collect()))
    }

    async fn dm(&self, user_id: &str, content: &str) -> bool {
        let send = async {
            let uid: u64 = user_id.parse().unwrap_or(0);
            let cached = self.dms.borrow().iter().find(|(u, _)| *u == uid).map(|(_, c)| *c);
            let channel = match cached {
                Some(c) => c.to_string(),
                None => {
                    let res = self.api(Method::POST, "/users/@me/channels", Some(json!({ "recipient_id": user_id }).to_string()), None).await?;
                    let id = serde_json::from_slice::<Id>(&res)?.id;
                    if let Ok(cid) = id.parse() {
                        let mut dms = self.dms.borrow_mut();
                        if dms.len() >= 1000 { dms.pop_front(); }
                        dms.push_back((uid, cid));
                    }
                    id
                }
            };
            let body = json!({ "content": content, "allowed_mentions": { "parse": [] } }).to_string();
            self.api(Method::POST, &format!("/channels/{channel}/messages"), Some(body), None).await?;
            Ok::<(), Error>(())
        };
        send.await.is_ok()
    }

    // ─── Starter message delivery ─────────────────────────────────────────────
    // A forum post's starter message arrives over the gateway (MESSAGE_CREATE, id === thread id)
    // at about the same moment as THREAD_CREATE, in either order. We pair them up instead of
    // fetching over REST; REST is only a fallback if the gateway copy doesn't show up in time.
    fn on_message_create(&self, m: Msg) {
        if m.id != m.channel_id { return; } // only forum/media post starter messages satisfy this
        let open = m.guild_id.is_some_and(|gid| self.db.borrow().guilds.get(gid).is_some_and(|g| g.phase != 0));
        if !open { return; }
        let content = m.content.and_then(|raw| serde_json::from_str::<Option<String>>(raw.get()).ok().flatten()).unwrap_or_default();
        let resolver = self.waiting.borrow_mut().remove(m.id);
        if let Some(tx) = resolver { let _ = tx.send(content); return; }
        let mut early = self.early.borrow_mut();
        early.retain(|_, (at, _)| at.elapsed() < Duration::from_secs(10));
        early.insert(m.id.to_owned(), (Instant::now(), content));
    }

    async fn starter_content(&self, id: &str, rest_fallback: bool) -> String {
        let arrived = self.early.borrow_mut().remove(id);
        if let Some((at, content)) = arrived { if at.elapsed() < Duration::from_secs(10) { return content; } }
        let (tx, rx) = oneshot::channel();
        self.waiting.borrow_mut().insert(id.to_owned(), tx);
        if let Ok(Ok(content)) = timeout(Duration::from_secs(1), rx).await { return content; }
        self.waiting.borrow_mut().remove(id);
        if !rest_fallback { return String::new(); }
        let fetched = self.api(Method::GET, &format!("/channels/{id}/messages/{id}"), None, None).await;
        fetched.ok().and_then(|b| serde_json::from_slice::<Content>(&b).ok()).and_then(|m| m.content).unwrap_or_default()
    }

    // ─── Moderation: only NEW posts; replies inside threads are never checked ──
    /// Cheap synchronous filter, so threads we don't care about never become a task.
    fn thread_checks(&self, t: &Thread) -> Option<(u8, String, Rc<Compiled>)> {
        if t.newly_created != Some(true) { return None; }
        let db = self.db.borrow();
        let g = db.guilds.get(t.guild_id.as_deref()?)?;
        let forum = g.forum_id.as_ref()?;
        if g.phase == 0 || t.parent_id.as_ref() != Some(forum) || t.owner_id.as_deref() == Some(self.me.borrow().as_str()) { return None; }
        let list = compile(g.lists.get(g.phase));
        if list.tags.is_empty() && !list.has_words() { return None; }
        Some((g.phase, forum.clone(), list))
    }

    async fn on_thread_create(&self, t: Thread, phase: u8, forum: String, list: Rc<Compiled>) -> Res<()> {
        let (guild_id, name, owner) = (t.guild_id.as_deref().unwrap_or(""), t.name.as_deref().unwrap_or(""), t.owner_id.as_deref().unwrap_or(""));

        // Tags and title are known right now, so check them with no network round trip.
        let tag = t.applied_tags.iter().flatten().find(|id| list.tags.contains(*id));
        let tag = tag.map(|id| self.db.borrow().guilds.get(guild_id).map_or_else(|| id.clone(), |g| self.tag_name(g, id)));
        let mut word = if tag.is_none() { find_word(name, &list) } else { None };
        let mut content = None;
        if tag.is_none() && word.is_none() {
            if !list.has_words() { return Ok(()); }
            let body = self.starter_content(&t.id, true).await;
            word = find_word(&body, &list);
            if word.is_none() { return Ok(()); }
            content = Some(body);
        }
        let word = word.unwrap_or_default();
        let (why, reason) = match &tag {
            Some(tag) => (format!("uses the tag **{tag}**"), format!("Phase {phase} blocklist: tag {tag}")),
            None => (format!("contains the blocked word **{word}**"), format!("Phase {phase} blocklist: word \"{word}\"")),
        };

        match self.api(Method::DELETE, &format!("/channels/{}", t.id), None, Some(&reason)).await {
            Err(e) if e.status == 404 => return Ok(()),
            other => { other?; }
        }

        let content = match content { // for the DM copy; thread is gone, so no REST
            Some(c) => c,
            None => self.starter_content(&t.id, false).await,
        };
        let mut msg = format!("Your post **{name}** in <#{forum}> was removed because it {why}, which isn't allowed right now.");
        if !content.is_empty() {
            msg.push_str("\nHere's a copy of what you wrote:\n>>> ");
            match truncated(&content, 1500) {
                Some(head) => { msg.push_str(head); msg.push('…'); }
                None => msg.push_str(&content),
            }
        }
        let sent = self.dm(owner, &msg).await;
        println!("[{guild_id}] deleted post \"{name}\" by {owner} ({}){}", why.replace('*', ""), if sent { "" } else { " — DM failed" });
        Ok(())
    }

    /// Edits only our bits on the members-role overwrite; uses the gateway cache (one REST call).
    async fn set_access(&self, forum: &str, role: &str, open: bool, reason: &str) -> Res<()> {
        let bits = |ows: &[Overwrite]| ows.iter().find(|o| o.id == role).map_or((0, 0), |o| (o.allow.parse::<u128>().unwrap_or(0), o.deny.parse::<u128>().unwrap_or(0)));
        let cached = self.forums.borrow().get(forum).map(|f| bits(&f.overwrites));
        let (mut allow, mut deny) = match cached {
            Some(b) => b,
            None => {
                let res = self.api(Method::GET, &format!("/channels/{forum}"), None, None).await?;
                bits(&serde_json::from_slice::<Overwrites>(&res)?.permission_overwrites.unwrap_or_default())
            }
        };
        allow |= READ; deny &= !READ;
        if open { allow |= WRITE; deny &= !WRITE; } else { allow &= !WRITE; deny |= WRITE; }
        let body = json!({ "type": 0, "allow": allow.to_string(), "deny": deny.to_string() }).to_string();
        self.api(Method::PUT, &format!("/channels/{forum}/permissions/{role}"), Some(body), Some(reason)).await?;
        Ok(())
    }

    // ─── Slash command handlers ───────────────────────────────────────────────
    async fn run_command(self: &Rc<Self>, i: &Interaction, p: &Parsed) -> Res<String> {
        let gid = i.guild_id.as_deref().unwrap_or("");
        let who = i.member.as_ref().and_then(|m| m.user.as_ref()).map_or("unknown", |u| u.username.as_str());
        let (forum, role) = {
            let mut db = self.db.borrow_mut();
            let g = db.guilds.entry(gid.to_owned()).or_default();
            (g.forum_id.clone(), g.role_id.clone().unwrap_or_default())
        };

        if p.name == "setup" {
            let (new_forum, new_role) = (p.str("forum"), p.str("role"));
            let changed = forum.as_deref().is_some_and(|f| f != new_forum);
            {
                let mut db = self.db.borrow_mut();
                let g = db.guilds.entry(gid.to_owned()).or_default();
                if changed { for n in [1, 2] { let l = g.lists.get_mut(n); l.tags.clear(); l.compiled.take(); } } // old forum's tag ids
                g.forum_id = Some(new_forum.to_owned());
                g.role_id = Some(new_role.to_owned());
                g.phase = 0;
            }
            self.save()?; self.update_warm();
            return Ok(format!("Managing <#{new_forum}> with members role <@&{new_role}>. Use `/open` when ready.{}",
                if changed { "\nBlocked tags were cleared since they belonged to the previous forum." } else { "" }));
        }
        if p.name == "status" {
            let Some(forum) = forum else { return Ok("Not set up yet — run `/setup`.".into()) };
            let db = self.db.borrow();
            let g = &db.guilds[gid];
            let state = if g.phase != 0 { format!("open (phase {})", g.phase) } else { "closed".into() };
            return Ok(format!("Forum: <#{forum}> · Role: <@&{role}> · State: **{state}**\n{}\n{}", self.describe_list(g, 1), self.describe_list(g, 2)));
        }
        let Some(forum) = forum else { return Ok("Run `/setup` first.".into()) };

        match p.name.as_str() {
            "open" => {
                let Some(phase) = p.phase() else { return Ok("Unknown command.".into()) };
                self.set_access(&forum, &role, true, &format!("Opened (phase {phase}) by {who}")).await?;
                if let Some(g) = self.db.borrow_mut().guilds.get_mut(gid) { g.phase = phase; }
                self.save()?; self.update_warm();
                let db = self.db.borrow();
                Ok(format!("<#{forum}> is **open — phase {phase}**. New posts are checked against:\n{}", self.describe_list(&db.guilds[gid], phase)))
            }
            "close" => {
                self.set_access(&forum, &role, false, &format!("Closed by {who}")).await?;
                if let Some(g) = self.db.borrow_mut().guilds.get_mut(gid) { g.phase = 0; }
                self.save()?; self.update_warm();
                Ok(format!("<#{forum}> is now **read-only** for <@&{role}>."))
            }
            "blocklist" => {
                let Some(phase) = p.phase() else { return Ok("Unknown command.".into()) };
                let (value, add) = (p.str("value"), p.sub.as_deref() == Some("add"));
                let v = value.trim().to_lowercase();
                {
                    let mut db = self.db.borrow_mut();
                    let Some(g) = db.guilds.get_mut(gid) else { return Ok("Run `/setup` first.".into()) };
                    if p.str("type") == "word" {
                        let words: Vec<String> = value.split(',').map(|w| w.trim().to_lowercase()).filter(|w| !w.is_empty()).collect();
                        let list = g.lists.get_mut(phase);
                        if add { for w in words { if !list.words.contains(&w) { list.words.push(w); } } }
                        else { list.words.retain(|w| !words.contains(w)); }
                    } else if add {
                        let found = self.forums.borrow().get(&forum).map(|f| f.tags.iter().find(|t| t.id == value || t.name.to_lowercase() == v).cloned().ok_or_else(|| f.tags.iter().map(|t| format!("`{}`", t.name)).collect::<Vec<_>>().join(", ")));
                        let tag = match found.unwrap_or_else(|| Err(String::new())) {
                            Ok(tag) => tag,
                            Err(available) => return Ok(format!("No tag `{value}` in <#{forum}>. Available: {}", if available.is_empty() { "_none_" } else { &available })),
                        };
                        let list = g.lists.get_mut(phase);
                        if !list.tags.iter().any(|t| t.id == tag.id) { list.tags.push(tag); }
                    } else {
                        let keep: Vec<bool> = g.lists.get(phase).tags.iter().map(|t| t.id != value && self.tag_name(g, &t.id).to_lowercase() != v).collect();
                        let mut keep = keep.into_iter();
                        g.lists.get_mut(phase).tags.retain(|_| keep.next().unwrap_or(true));
                    }
                    g.lists.get_mut(phase).compiled.take();
                }
                self.save()?;
                let db = self.db.borrow();
                Ok(format!("Updated.\n{}", self.describe_list(&db.guilds[gid], phase)))
            }
            _ => Ok("Unknown command.".into()),
        }
    }

    fn autocomplete_choices(&self, i: &Interaction, p: &Parsed) -> Vec<Value> {
        let query = p.focused.as_ref().map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_owned)).unwrap_or_default().to_lowercase();
        let db = self.db.borrow();
        let mut choices: Vec<(String, String)> = Vec::new(); // (name, value)
        let guild = i.guild_id.as_ref().and_then(|gid| db.guilds.get(gid));
        if let (Some(g), Some(phase), kind) = (guild, p.phase(), p.str("type")) {
            if let (Some(forum), false) = (&g.forum_id, kind.is_empty()) {
                let list = g.lists.get(phase);
                if p.sub.as_deref() == Some("add") {
                    if kind == "tag" {
                        let forums = self.forums.borrow();
                        let tags = forums.get(forum).map(|f| f.tags.as_slice()).unwrap_or_default();
                        choices = tags.iter().filter(|t| !list.tags.iter().any(|x| x.id == t.id)).map(|t| (t.name.clone(), t.id.clone())).collect();
                    }
                } else if kind == "tag" {
                    choices = list.tags.iter().map(|t| (self.tag_name(g, &t.id), t.id.clone())).collect();
                } else {
                    choices = list.words.iter().map(|w| (w.clone(), w.clone())).collect();
                }
            }
        }
        choices.into_iter().filter(|(name, _)| name.to_lowercase().contains(&query)).take(25).map(|(name, value)| json!({ "name": name, "value": value })).collect()
    }

    async fn on_interaction(self: Rc<Self>, mut i: Interaction) {
        let Some(data) = i.data.take() else { return };
        let p = Parsed::new(data);
        let callback = format!("/interactions/{}/{}/callback", i.id, i.token);
        if i.kind == 4 {
            let body = json!({ "type": 8, "data": { "choices": self.autocomplete_choices(&i, &p) } }).to_string();
            if let Err(e) = self.api(Method::POST, &callback, Some(body), None).await { eprintln!("INTERACTION_CREATE: {e}"); }
            return;
        }
        if i.kind != 2 || i.guild_id.is_none() { return; }
        let reply = |content: String| {
            let body = json!({ "type": 4, "data": { "content": content, "flags": 64, "allowed_mentions": { "parse": [] } } }).to_string();
            self.api(Method::POST, &callback, Some(body), None)
        };
        let failed = |e: &Error| format!("Failed: {}", truncated(&e.msg, 1800).unwrap_or(&e.msg));
        let result = match self.run_command(&i, &p).await {
            Ok(content) => reply(content).await.map(drop),
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            eprintln!("{e}");
            let _ = reply(failed(&e)).await;
        }
    }

    async fn register_commands(self: Rc<Self>, app_id: String) -> Res<()> {
        let commands = commands_json();
        let hash = format!("{app_id}:{:016x}", fnv1a(&commands));
        if self.db.borrow().commands_hash.as_deref() == Some(&hash) { return Ok(()); } // unchanged since last run, so skip the API call
        self.api(Method::PUT, &format!("/applications/{app_id}/commands"), Some(commands), None).await?;
        self.db.borrow_mut().commands_hash = Some(hash);
        self.save()?;
        println!("Slash commands registered");
        Ok(())
    }

    // ─── Gateway ──────────────────────────────────────────────────────────────
    fn cache_forum(&self, ch: &Channel) {
        if ch.kind != Some(15) { return; }
        let tags = ch.available_tags.and_then(|r| serde_json::from_str::<Option<Vec<Tag>>>(r.get()).ok().flatten()).unwrap_or_default();
        let overwrites = ch.permission_overwrites.and_then(|r| serde_json::from_str::<Option<Vec<Overwrite>>>(r.get()).ok().flatten()).unwrap_or_default();
        self.forums.borrow_mut().insert(ch.id.to_owned(), Forum { tags, overwrites });
    }

    fn dispatch(self: &Rc<Self>, t: &str, d: &str) -> Result<(), serde_json::Error> {
        match t {
            "MESSAGE_CREATE" => self.on_message_create(serde_json::from_str(d)?), // most frequent event first
            "THREAD_CREATE" => {
                let thread: Thread = serde_json::from_str(d)?;
                if let Some((phase, forum, list)) = self.thread_checks(&thread) {
                    let bot = self.clone();
                    spawn_local(async move { if let Err(e) = bot.on_thread_create(thread, phase, forum, list).await { eprintln!("THREAD_CREATE: {e}"); } });
                }
            }
            "INTERACTION_CREATE" => { spawn_local(self.clone().on_interaction(serde_json::from_str(d)?)); }
            "GUILD_CREATE" => {
                serde_json::from_str::<GuildCreate>(d)?.channels.iter().for_each(|ch| self.cache_forum(ch));
                release_free_memory(); // the payload can be large; hand its pages back to the OS
            }
            "CHANNEL_CREATE" | "CHANNEL_UPDATE" => self.cache_forum(&serde_json::from_str(d)?),
            "CHANNEL_DELETE" => { self.forums.borrow_mut().remove(&serde_json::from_str::<Id>(d)?.id); }
            "READY" => {
                let r: Ready = serde_json::from_str(d)?;
                println!("Logged in as {}", r.user.username);
                *self.me.borrow_mut() = r.user.id;
                *self.session.borrow_mut() = Some((r.session_id, r.resume_gateway_url));
                let bot = self.clone();
                spawn_local(async move { let _ = bot.request(&Method::GET, "/gateway", None, None).await; }); // pre-warm the REST connection
                self.update_warm();
                let bot = self.clone();
                spawn_local(async move { if let Err(e) = bot.register_commands(r.application.id).await { eprintln!("READY: {e}"); } });
            }
            _ => {}
        }
        Ok(())
    }

    /// Runs one gateway connection until it ends; returns the close code (1006 = dropped).
    async fn gateway_session(self: &Rc<Self>) -> u16 {
        let resume = self.session.borrow().clone();
        let base = resume.as_ref().map_or("wss://gateway.discord.gg", |(_, url)| url.as_str()).trim_end_matches('/');
        let host = base.strip_prefix("wss://").unwrap_or(base).split(['/', ':']).next().unwrap_or_default();
        let config = WebSocketConfig::default().read_buffer_size(16 * 1024).write_buffer_size(0);
        let connect = async {
            let io = self.tls_connect(host).await.map_err(|e| e.to_string())?;
            tokio_tungstenite::client_async_with_config(format!("{base}/?v=10&encoding=json"), io, Some(config)).await.map_err(|e| e.to_string())
        };
        let mut ws = match timeout(Duration::from_secs(20), connect).await {
            Ok(Ok((ws, _))) => ws,
            Ok(Err(e)) => { eprintln!("Gateway connect failed: {e}"); return 1006; }
            Err(_) => { eprintln!("Gateway connect timed out"); return 1006; }
        };

        let mut heartbeat: Option<tokio::time::Interval> = None;
        let mut acked = true;
        macro_rules! send { ($payload:expr) => { if ws.send(Message::text($payload.to_string())).await.is_err() { return 1006; } } }
        macro_rules! close { () => {{
            let frame = CloseFrame { code: CloseCode::from(4000), reason: "".into() }; // not 1000, so the session stays resumable
            let _ = timeout(Duration::from_secs(1), ws.close(Some(frame))).await;
            return 4000;
        }} }
        loop {
            tokio::select! {
                frame = ws.next() => {
                    let text = match frame {
                        Some(Ok(Message::Text(text))) => text,
                        Some(Ok(Message::Close(frame))) => return frame.map_or(1005, |f| f.code.into()),
                        Some(Ok(_)) => continue,
                        Some(Err(_)) | None => return 1006,
                    };
                    let Ok(msg) = serde_json::from_str::<Envelope>(text.as_str()) else { continue };
                    if msg.s.is_some() { self.seq.set(msg.s); }
                    let d = msg.d.map_or("null", RawValue::get);
                    match msg.op {
                        0 => {
                            let t = msg.t.unwrap_or_default();
                            if let Err(e) = self.dispatch(t, d) { eprintln!("{t}: {e}"); }
                        }
                        1 => send!(json!({ "op": 1, "d": self.seq.get() })),
                        7 => close!(),                                                        // reconnect requested
                        9 => { if d != "true" { self.session.take(); } close!() }              // invalid session
                        11 => acked = true,
                        10 => {
                            let Ok(hello) = serde_json::from_str::<Hello>(d) else { continue };
                            let period = Duration::from_millis(hello.heartbeat_interval.max(1000));
                            let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period.mul_f64(rand01()), period);
                            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                            heartbeat = Some(interval);
                            match &resume {
                                Some((session_id, _)) => send!(json!({ "op": 6, "d": { "token": self.token, "session_id": session_id, "seq": self.seq.get() } })),
                                None => send!(json!({ "op": 2, "d": { "token": self.token, "intents": INTENTS, "properties": { "os": std::env::consts::OS, "browser": "botrd", "device": "botrd" } } })),
                            }
                        }
                        _ => {}
                    }
                }
                _ = async { match heartbeat.as_mut() { Some(i) => { i.tick().await; } None => std::future::pending().await } } => {
                    if !acked { close!() } // zombie → reconnect
                    acked = false;
                    send!(json!({ "op": 1, "d": self.seq.get() }));
                }
            }
        }
    }

    async fn run(self: Rc<Self>) {
        loop {
            let code = self.gateway_session().await;
            if [4004, 4010, 4011, 4012, 4013, 4014].contains(&code) {
                eprintln!("Fatal gateway close {code}{}", if code == 4014 { " — enable Message Content Intent in the developer portal" } else { "" });
                std::process::exit(1);
            }
            if code == 4007 || code == 4009 { self.session.take(); }
            eprintln!("Gateway closed ({code}), reconnecting…");
            sleep(Duration::from_secs_f64(1.0 + rand01() * 4.0)).await;
        }
    }
}

fn main() {
    let env = env_lookup();
    let token = env("DISCORD_TOKEN").unwrap_or_default();
    let auth = HeaderValue::from_str(&format!("Bot {token}")).ok().filter(|_| !token.is_empty());
    let Some(auth) = auth else { eprintln!("DISCORD_TOKEN is not set (put it in .env)"); std::process::exit(1) };
    let data_file = env("DATA_FILE").unwrap_or_else(|| "data.json".into());

    let bot = Bot::new(token, auth, data_file);

    // One thread for everything; the single extra "blocking" thread is only used for DNS lookups and exits when idle.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all()
        .max_blocking_threads(1).thread_keep_alive(Duration::from_secs(1)).build().expect("tokio runtime");
    tokio::task::LocalSet::new().block_on(&runtime, bot.run());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(words: &[&str]) -> Rc<Compiled> {
        compile(&List { words: words.iter().map(|w| w.to_string()).collect(), ..Default::default() })
    }

    #[test]
    fn whole_words_only() {
        let l = list(&["ass", "free nitro"]);
        assert_eq!(find_word("a class act", &l), None);
        assert_eq!(find_word("what an ASS!", &l).as_deref(), Some("ass"));
        assert_eq!(find_word("get Free-Nitro now", &l).as_deref(), Some("free nitro"));
        assert_eq!(find_word("free the nitro", &l), None);
        assert_eq!(find_word("", &l), None);
    }

    #[test]
    fn symbols_match_as_substrings() {
        let l = list(&["c++", "🍆"]);
        assert_eq!(find_word("I love C++20", &l).as_deref(), Some("c++"));
        assert_eq!(find_word("look🍆here", &l).as_deref(), Some("🍆"));
        assert_eq!(find_word("plain c", &l), None);
    }

    #[test]
    fn unicode_words() {
        let l = list(&["привет", "don't"]);
        assert_eq!(find_word("ПРИВЕТ, мир", &l).as_deref(), Some("привет"));
        assert_eq!(find_word("приветствие", &l), None);
        assert_eq!(find_word("please DON'T do it", &l).as_deref(), Some("don't"));
    }

    #[test]
    fn data_file_round_trip_and_v1_migration() {
        let v2 = r#"{"guilds":{"1":{"forumId":"2","roleId":"3","phase":1,"lists":{"1":{"words":["a"],"tags":[{"id":"9","name":"t"}]},"2":{"words":[],"tags":[]}}}},"commandsHash":"x"}"#;
        let db: Db = serde_json::from_str(v2).unwrap();
        assert_eq!(serde_json::to_value(&db).unwrap(), serde_json::from_str::<Value>(v2).unwrap());

        let dir = std::env::temp_dir().join(format!("botrd-test-{}", std::process::id()));
        std::fs::write(&dir, r#"{"1":{"forumId":"2","roleId":"3","phase":2,"lists":{"1":{"words":[],"tags":[]},"2":{"words":["b"],"tags":[]}}}}"#).unwrap();
        let db = load_db(dir.to_str().unwrap());
        std::fs::remove_file(&dir).unwrap();
        assert_eq!(db.guilds["1"].phase, 2);
        assert_eq!(db.guilds["1"].lists.two.words, ["b"]);
    }

    /// Talks to the real Discord API (no token needed): `cargo test -- --ignored`
    #[test]
    #[ignore]
    fn rest_pool_reuses_connections() {
        let bot = Bot::new("x".into(), HeaderValue::from_static("Bot x"), "/nonexistent/data.json".into());
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async {
            for _ in 0..3 {
                let body = bot.api(Method::GET, "/gateway", None, None).await.unwrap();
                assert!(String::from_utf8_lossy(&body).contains("wss://"));
                assert_eq!(bot.pool.borrow().len(), 1);
            }
            let err = bot.api(Method::GET, "/users/@me", None, None).await.unwrap_err();
            assert_eq!(err.status, 401);
            let (a, b) = tokio::join!(bot.api(Method::GET, "/gateway", None, None), bot.api(Method::GET, "/gateway", None, None));
            assert!(a.is_ok() && b.is_ok());
            assert_eq!(bot.pool.borrow().len(), 2);
        });
    }

    #[test]
    fn helpers() {
        assert_eq!(pct("Phase 1: word \"é\""), "Phase%201%3A%20word%20%22%C3%A9%22");
        assert_eq!(truncated("abc", 3), None);
        assert_eq!(truncated("ab😀c", 3), Some("ab"));
        let cmds: Value = serde_json::from_str(&commands_json()).unwrap();
        assert_eq!(cmds.as_array().unwrap().len(), 5);
        assert_eq!(cmds[4]["options"][0]["options"][2]["autocomplete"], true);
    }
}
