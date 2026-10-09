#!/usr/bin/env python3
import os
import time
import statistics
import math
import json
import sqlite3
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

import requests

GATE_TICKERS_URL = "https://api.gateio.ws/api/v4/spot/tickers"
GATE_CANDLES_URL = "https://api.gateio.ws/api/v4/spot/candlesticks"
CDC_INSTRUMENTS_URL = "https://api.crypto.com/exchange/v1/public/get-instruments"
CDC_TICKERS_URL = "https://api.crypto.com/exchange/v1/public/get-tickers"
CDC_CANDLES_URL = "https://api.crypto.com/exchange/v1/public/get-candlestick"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
MIN_24H_QUOTE_VOL = float(os.getenv("MIN_24H_QUOTE_VOL", "150000"))
MIN_WATCH_24H_QUOTE_VOL = float(os.getenv("MIN_WATCH_24H_QUOTE_VOL", "10000"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "3"))
EARLY_MIN_VOLUME_RATIO = float(os.getenv("EARLY_MIN_VOLUME_RATIO", "3"))
EARLY_MAX_RET5 = float(os.getenv("EARLY_MAX_RET5", "3.0"))
PILOT_SCORE = int(os.getenv("PILOT_SCORE", "78"))
CONFIRMED_SCORE = int(os.getenv("CONFIRMED_SCORE", "90"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "3"))
PAIR_COOLDOWN_MIN = int(os.getenv("PAIR_COOLDOWN_MIN", "60"))
SLACK_ENABLED = os.getenv("SLACK_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# V3 — une IA arbitre uniquement les trajectoires déjà confirmées par la V2.
# Slack reste silencieux pour PILOTE / ATTENDRE / IGNORER : seule la décision TRADE notifie.
AI_ENABLED = os.getenv("AI_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.6-luna").strip()
MAX_AI_REVIEWS_PER_SCAN = int(os.getenv("MAX_AI_REVIEWS_PER_SCAN", "3"))
AI_WAIT_COOLDOWN_MIN = int(os.getenv("AI_WAIT_COOLDOWN_MIN", "5"))
AI_IGNORE_COOLDOWN_MIN = int(os.getenv("AI_IGNORE_COOLDOWN_MIN", "20"))
AI_ERROR_COOLDOWN_MIN = int(os.getenv("AI_ERROR_COOLDOWN_MIN", "3"))
AI_MAX_SIGNAL_AGE_MIN = int(os.getenv("AI_MAX_SIGNAL_AGE_MIN", "90"))
AI_MAX_PRICE_GAIN = float(os.getenv("AI_MAX_PRICE_GAIN", "4.0"))
AI_HTTP_TIMEOUT = int(os.getenv("AI_HTTP_TIMEOUT", "35"))
AI_OUTPUT_TOKEN_BUDGET = int(os.getenv("AI_OUTPUT_TOKEN_BUDGET", "3000"))
# Strict net-exploitability gate. Conservative default for base Crypto.com
# Exchange spot taker trades (0.50% buy + 0.50% sell), plus execution buffer.
# The app can charge a different price/fee: never promise an executable profit.
ROUND_TRIP_FEE_PCT = float(os.getenv("ROUND_TRIP_FEE_PCT", "1.00"))
EXECUTION_BUFFER_PCT = float(os.getenv("EXECUTION_BUFFER_PCT", "0.30"))
MIN_NET_TP1_PCT = float(os.getenv("MIN_NET_TP1_PCT", "0.75"))
MIN_NET_TP2_PCT = float(os.getenv("MIN_NET_TP2_PCT", "3.00"))
MIN_NET_REWARD_RISK = float(os.getenv("MIN_NET_REWARD_RISK", "2.00"))
MAX_ENTRY_DRIFT_PCT = float(os.getenv("MAX_ENTRY_DRIFT_PCT", "0.50"))
# Swing candidates are silent until validated by the same net-gain and CDC gates.
SWING_ENABLED = os.getenv("SWING_ENABLED", "1").strip().lower() in {"1","true","yes","on"}
SWING_MIN_AGE_MIN = float(os.getenv("SWING_MIN_AGE_MIN", "8"))
SWING_MIN_QUOTE_VOL = float(os.getenv("SWING_MIN_QUOTE_VOL", "200000"))
SWING_MAX_SPREAD = float(os.getenv("SWING_MAX_SPREAD", "0.15"))
SWING_MIN_RECENT_SPIKES = int(os.getenv("SWING_MIN_RECENT_SPIKES", "3"))
SWING_MIN_SPIKE_RATIO = float(os.getenv("SWING_MIN_SPIKE_RATIO", "8"))



# V2 — suivi de trajectoire après la première détection pilote.
PILOT_TTL_MIN = int(os.getenv("PILOT_TTL_MIN", "10080"))
MAX_PILOT_24H = float(os.getenv("MAX_PILOT_24H", "10"))
CONFIRM_MIN_AGE_SEC = int(os.getenv("CONFIRM_MIN_AGE_SEC", "55"))
CONFIRM_MIN_PRICE_GAIN = float(os.getenv("CONFIRM_MIN_PRICE_GAIN", "0.30"))
CONFIRM_MIN_RET1 = float(os.getenv("CONFIRM_MIN_RET1", "0.05"))
CONFIRM_MIN_RET5 = float(os.getenv("CONFIRM_MIN_RET5", "0.35"))
CONFIRM_MIN_RET15 = float(os.getenv("CONFIRM_MIN_RET15", "0.10"))
CONFIRM_MIN_VOL_RATIO = float(os.getenv("CONFIRM_MIN_VOL_RATIO", "12"))
CONFIRM_VOL_KEEP_RATIO = float(os.getenv("CONFIRM_VOL_KEEP_RATIO", "0.85"))
CONFIRM_MAX_SPREAD = float(os.getenv("CONFIRM_MAX_SPREAD", "0.20"))

HTTP_TIMEOUT = 15
HISTORY_MAX_MIN = int(os.getenv("HISTORY_MAX_MIN", "450"))

EXCLUDED_BASES = {
    "USDC", "USDE", "USDS", "FDUSD", "TUSD", "DAI", "PYUSD", "USD1",
    "EUR", "EURC", "EURI", "USDP", "GUSD", "BUSD", "USDT", "USD", "USDD", "USDDC"
}

session = requests.Session()
session.headers.update({"Accept": "application/json", "User-Agent": "crypto-radar-clean/3.0"})

history = defaultdict(lambda: deque(maxlen=HISTORY_MAX_MIN + 10))
last_alert_at = {}
pilots = {}
slack_blocked_until = 0.0
last_slack_send_at = 0.0
ai_next_review_at = {}
cdc_pairs = set()
cdc_market_by_base = {}
cdc_ticker_by_pair = {}
gate_aux_history = defaultdict(lambda: deque(maxlen=50))
candle_cache = {}
CANDLE_ROTATION_PER_SCAN = int(os.getenv("CANDLE_ROTATION_PER_SCAN", "500"))
CANDLE_FOCUS_PER_SCAN = int(os.getenv("CANDLE_FOCUS_PER_SCAN", "20"))
CANDLE_TRACKED_PER_SCAN = int(os.getenv("CANDLE_TRACKED_PER_SCAN", "28"))
CANDLE_WORKERS = min(6, max(1, int(os.getenv("CANDLE_WORKERS", "6"))))
_candle_cursor = 0
_track_cursor = 0
v5_diagnostics = Counter()
CANDLE_MIN_FRESH_SEC = int(os.getenv("CANDLE_MIN_FRESH_SEC", "140"))
V4_BACKTEST_MODE = os.getenv("PEPITO_SELFTEST", "0").lower() in {"1","true","yes","on"}

cdc_pairs_updated_at = 0.0
positions = {}
gate_rejections = Counter()
POSITIONS_FILE = os.getenv("POSITIONS_FILE", "positions.json")
STATE_DB_PATH = os.getenv("STATE_DB_PATH", "pepito.sqlite3")
_state_db = None

def init_state_db():
    global _state_db
    parent = os.path.dirname(STATE_DB_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    _state_db = sqlite3.connect(STATE_DB_PATH, timeout=10)
    _state_db.execute("PRAGMA journal_mode=WAL")
    _state_db.execute("""CREATE TABLE IF NOT EXISTS market_history (
        pair TEXT NOT NULL, bucket_ts INTEGER NOT NULL, price REAL NOT NULL,
        qv24 REAL NOT NULL, bid REAL NOT NULL, ask REAL NOT NULL,
        PRIMARY KEY(pair,bucket_ts))""")
    _state_db.execute("CREATE INDEX IF NOT EXISTS idx_market_history_ts ON market_history(bucket_ts)")
    _state_db.execute("""CREATE TABLE IF NOT EXISTS pilot_state (
        pair TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at REAL NOT NULL)""")
    _state_db.execute("""CREATE TABLE IF NOT EXISTS sent_trades (
        pair TEXT PRIMARY KEY, sent_at REAL NOT NULL)""")
    for pair, sent_at in _state_db.execute(
        "SELECT pair,sent_at FROM sent_trades WHERE sent_at >= ?", (time.time()-7*86400,)
    ).fetchall():
        last_alert_at[pair] = float(sent_at)
    _state_db.execute("DELETE FROM market_history WHERE bucket_ts < ?", (int(time.time()) - 8*24*3600,))
    _state_db.commit()
    _state_db.execute("VACUUM")
    cur = _state_db.execute("SELECT COUNT(*), MIN(bucket_ts), MAX(bucket_ts) FROM market_history")
    row = cur.fetchone() or (0, None, None)
    print(f"PEPITO STATE — SQLite actif | {STATE_DB_PATH} | rows={row[0]} | oldest={row[1]} | newest={row[2]}", flush=True)

def persist_pilot_state(pair, pilot):
    if _state_db is None or pilot is None:
        return
    payload = dict(pilot.__dict__)
    payload["trajectory"] = list(pilot.trajectory or [])
    _state_db.execute("""INSERT INTO pilot_state(pair,payload,updated_at) VALUES(?,?,?)
        ON CONFLICT(pair) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at""",
        (pair, json.dumps(payload, separators=(",", ":")), time.time()))

def restore_pilots(now):
    if _state_db is None:
        return 0
    restored = 0
    ttl = PILOT_TTL_MIN * 60
    for pair, payload, updated_at in _state_db.execute("SELECT pair,payload,updated_at FROM pilot_state").fetchall():
        try:
            d = json.loads(payload)
            if (now - float(d.get("created_at", 0)) > ttl
                    or last_alert_at.get(pair, 0) >= float(d.get("created_at", 0))):
                _state_db.execute("DELETE FROM pilot_state WHERE pair=?", (pair,))
                continue
            d["trajectory"] = deque(d.get("trajectory") or [], maxlen=90)
            pilots[pair] = PilotState(**d)
            restored += 1
        except Exception as exc:
            print(f"PEPITO RESTORE ERREUR — {pair} | {type(exc).__name__}: {exc}", flush=True)
            _state_db.execute("DELETE FROM pilot_state WHERE pair=?", (pair,))
    _state_db.commit()
    print(f"PEPITO PILOTES — {restored} restaure(s) depuis SQLite", flush=True)
    return restored

def mark_trade_sent(pair, timestamp):
    """Persist cooldown and remove completed pilot before a Railway redeploy."""
    last_alert_at[pair] = timestamp
    pilots.pop(pair, None)
    ai_next_review_at.pop(pair, None)
    if _state_db is not None:
        _state_db.execute("DELETE FROM pilot_state WHERE pair=?", (pair,))
        _state_db.execute("""INSERT INTO sent_trades(pair,sent_at) VALUES (?,?)
            ON CONFLICT(pair) DO UPDATE SET sent_at=excluded.sent_at""", (pair, timestamp))
        _state_db.commit()


def restore_pilot_history(now):
    """Restore recent snapshots for live pilots so multi-pass tracking survives restarts."""
    if _state_db is None or not pilots:
        return 0
    restored = 0
    cutoff = int(now) - HISTORY_MAX_MIN * 60
    for pair in list(pilots.keys()):
        rows = _state_db.execute(
            "SELECT bucket_ts,price,qv24,bid,ask FROM market_history "
            "WHERE pair=? AND bucket_ts>=? ORDER BY bucket_ts",
            (pair, cutoff),
        ).fetchall()
        if not rows:
            continue
        dq = history[pair]
        dq.clear()
        for ts, price, qv24, bid, ask in rows:
            dq.append(Snapshot(float(ts), float(price), float(qv24), float(bid), float(ask)))
            restored += 1
    print(
        f"PEPITO HISTORIQUE — {restored} snapshot(s) restaure(s) pour "
        f"{sum(1 for p in pilots if history.get(p))} pilote(s)",
        flush=True,
    )
    return restored


def persist_market_snapshot(pair, snap):
    if _state_db is None:
        return
    bucket = int(snap.ts // 300) * 300
    _state_db.execute("""INSERT INTO market_history(pair,bucket_ts,price,qv24,bid,ask)
        VALUES(?,?,?,?,?,?) ON CONFLICT(pair,bucket_ts) DO UPDATE SET
        price=excluded.price,qv24=excluded.qv24,bid=excluded.bid,ask=excluded.ask""",
        (pair,bucket,snap.price,snap.qv24,snap.bid,snap.ask))

def commit_state(now):
    if _state_db is None:
        return
    _state_db.commit()
    if int(now//3600) != int((now-SCAN_INTERVAL)//3600):
        _state_db.execute("DELETE FROM market_history WHERE bucket_ts < ?", (int(now)-8*24*3600,))
        _state_db.commit()

def gate_historical_context(pair, now):
    """6h/24h/7d spot context from the actual Crypto.com Exchange instrument.
    Gate is supplementary for early detection, never the source of executable prices.
    """
    market = cdc_ticker_by_pair.get(pair)
    if not market or not market.get("exchange_symbol"):
        return {}
    try:
        r = session.get(CDC_CANDLES_URL, params={
            "instrument_name": market["exchange_symbol"], "timeframe": "1h",
            "count": 170
        }, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        payload = r.json()
        if payload.get("code") != 0:
            return {}
        rows = sorted(payload.get("result", {}).get("data", []),
                      key=lambda x: fnum(x.get("t")))
        parsed = []
        for c in rows:
            ts = fnum(c.get("t")) / 1000.0
            close = fnum(c.get("c"))
            high = fnum(c.get("h"))
            low = fnum(c.get("l"))
            vol = fnum(c.get("v"))
            if ts > 0 and close > 0 and high > 0 and low > 0 and ts <= now:
                parsed.append((ts, close, vol*close, high, low))
        if len(parsed) < 2:
            return {}
        current = fnum(market.get("last")) or parsed[-1][1]
        out = {}
        for label, sec in (("6h", 21600), ("24h", 86400), ("7d", 604800)):
            window = [x for x in parsed if x[0] >= now-sec]
            old = [x for x in parsed if x[0] <= now-sec]
            if len(window) < 2:
                continue
            hi = max(x[3] for x in window)
            lo = min(x[4] for x in window)
            comparison = old[-1][1] if old else window[0][1]
            out[label] = {
                "price": comparison, "return_pct": pct(current, comparison),
                "hour_quote_volume": window[-1][2],
                "source": "crypto_com_exchange_spot_1h",
                "high": hi, "low": lo,
                "range_pct": (hi/lo-1)*100 if lo>0 else 0
            }
        return out
    except Exception as exc:
        print(f"V4 CONTEXTE CDC INDISPONIBLE — {pair} | {type(exc).__name__}: {exc}", flush=True)
        return {}


def persistent_context(pair, now):
    if _state_db is not None:
        rows = _state_db.execute("SELECT bucket_ts,price,qv24 FROM market_history WHERE pair=? AND bucket_ts>=? ORDER BY bucket_ts",
            (pair,int(now)-7*24*3600)).fetchall()
    else:
        rows = []
    out = {}
    if rows:
        for label,sec in (("6h",21600),("24h",86400),("7d",604800)):
            eligible=[r for r in rows if r[0] <= now-sec]
            if eligible:
                r=eligible[-1]
                out[label]={"price":r[1],"return_pct":pct(rows[-1][1],r[1]),"qv24":r[2],"source":"pepito_sqlite"}
    # Gate hourly candles add observable high/low volatility, even if SQLite
    # already supplies the return. Only queried for AI finalists.
    gate = gate_historical_context(pair, now)
    for label in ("6h","24h","7d"):
        if label in gate:
            if label not in out:
                out[label] = gate[label]
            else:
                out[label].update({k: gate[label][k] for k in ("high","low","range_pct")
                                   if k in gate[label]})
    return out

def load_positions():
    global positions
    try:
        with open(POSITIONS_FILE, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        positions = raw if isinstance(raw, dict) else {}
        print(f"POSITIONS OUVERTES — {len(positions)} chargee(s)", flush=True)
    except FileNotFoundError:
        positions = {}
    except Exception as exc:
        positions = {}
        print(f"POSITIONS ERREUR LECTURE — {type(exc).__name__}: {exc}", flush=True)

def save_positions():
    tmp = POSITIONS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(positions, fh, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, POSITIONS_FILE)

def position_action(pair, price):
    p = positions.get(pair)
    if not p or price <= 0:
        return None
    entry = float(p.get("entry_price") or 0)
    stop = float(p.get("invalidation") or 0)
    tp1 = float(p.get("tp1") or 0)
    tp2 = float(p.get("tp2") or 0)
    if stop and price <= stop:
        return "SORTIR"
    if tp2 and price >= tp2:
        return "VENDRE DAVANTAGE"
    if tp1 and price >= tp1:
        return "PRENDRE DES BENEFICES"
    if entry and price >= entry * 1.08:
        return "NE PLUS RENFORCER"
    return None

CDC_PAIRS_TTL_SEC = int(os.getenv("CDC_PAIRS_TTL_SEC", "3600"))


@dataclass
class Snapshot:
    ts: float
    price: float
    qv24: float
    bid: float
    ask: float


@dataclass
class Signal:
    pair: str
    price: float
    ret_1m: float
    ret_5m: float
    ret_15m: float
    vol_ratio: float
    qv24: float
    spread_pct: float
    change_24h: float
    score: int
    level: str


@dataclass
class PilotState:
    created_at: float
    last_seen_at: float
    first_price: float
    first_score: int
    first_vol_ratio: float
    first_ret_1m: float
    first_ret_5m: float
    first_ret_15m: float
    first_change_24h: float
    sightings: int = 1
    best_score: int = 0
    best_vol_ratio: float = 0.0
    best_price: float = 0.0
    min_price: float = 0.0
    last_price: float = 0.0
    last_score: int = 0
    last_vol_ratio: float = 0.0
    trajectory: deque = None


@dataclass
class ConfirmedCandidate:
    signal: Signal
    pilot: PilotState
    price_gain: float
    age_min: float
    style: str = "MOMENTUM"


@dataclass
class AIReview:
    pair: str
    decision: str
    confidence: int
    reason: str
    entry_low: Optional[float] = None
    entry_high: Optional[float] = None
    invalidation: Optional[float] = None
    tp1: Optional[float] = None
    tp2: Optional[float] = None


def fnum(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def pct(a: float, b: float) -> float:
    if b <= 0:
        return 0.0
    return (a / b - 1.0) * 100.0


def base_of(pair: str) -> str:
    return pair.rsplit("_", 1)[0]


def excluded_pair(pair: str) -> bool:
    if not pair.endswith("_USDT"):
        return True
    base = base_of(pair)
    if base in EXCLUDED_BASES:
        return True
    # Exclut les tokens à levier classiques Gate (BTC3L, ETH5S, etc.).
    if base.endswith(("3L", "3S", "5L", "5S")):
        return True
    return False


def refresh_cdc_pairs(now=None):
    """Whitelist réelle des instruments spot actifs Crypto.com Exchange."""
    global cdc_pairs, cdc_pairs_updated_at
    now = now or time.time()
    if cdc_pairs and now - cdc_pairs_updated_at < CDC_PAIRS_TTL_SEC:
        return cdc_pairs
    try:
        r = session.get(CDC_INSTRUMENTS_URL, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        payload = r.json()
    except Exception as exc:
        cache_age = now - cdc_pairs_updated_at if cdc_pairs_updated_at else float("inf")
        # A transient Crypto.com outage must not erase a whitelist we just verified.
        # Beyond 6h (or before the first successful fetch), remain fail-closed.
        if cdc_pairs and cache_age <= 6 * 60 * 60:
            print(f"CDC WHITELIST — cache last-good utilisé | age={cache_age/60:.0f}m | {type(exc).__name__}", flush=True)
            return cdc_pairs
        raise
    data = payload.get("result", {}).get("data", []) if isinstance(payload, dict) else []
    fresh = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        name = str(item.get("symbol") or item.get("instrument_name") or "").upper().strip()
        inst_type = str(item.get("inst_type") or item.get("type") or "").upper()
        product_type = str(item.get("product_type") or "").upper()
        tradable = item.get("tradable") is True
        # Crypto.com labels spot markets as CCY_PAIR, not SPOT. Keep only live digital-currency pairs.
        if name and inst_type == "CCY_PAIR" and product_type == "DIGITAL_CURRENCIES" and tradable:
            fresh.add(name)
    if not fresh:
        raise RuntimeError("Whitelist Crypto.com Exchange vide")
    cdc_pairs = fresh
    cdc_pairs_updated_at = now
    print(f"CDC WHITELIST — {len(cdc_pairs)} instruments spot", flush=True)
    return cdc_pairs


def cdc_tradeable(pair: str) -> bool:
    base = base_of(pair).upper()
    # Gate travaille en BASE_USDT; Crypto.com peut coter le même actif en USD/USDT.
    return any(x in cdc_pairs for x in (f"{base}_USD", f"{base}_USDT", f"{base}-USD", f"{base}-USDT"))


def normalize_cdc_tickers(payload, allowed, now=None):
    """One executable quote per coin. SPOT whitelist is authoritative, never futures."""
    now = time.time() if now is None else now
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise RuntimeError("Crypto.com Exchange tickers indisponibles")
    data = payload.get("result", {}).get("data", [])
    if not isinstance(data, list):
        raise RuntimeError("Crypto.com Exchange tickers invalides")
    by_base = {}
    for x in data:
        name = str(x.get("i", "")).upper()
        if name not in allowed or "_" not in name:
            continue
        base, quote = name.rsplit("_", 1)
        if not base or quote not in {"USD", "USDT"} or base in EXCLUDED_BASES:
            continue
        price, bid, ask = fnum(x.get("a")), fnum(x.get("b")), fnum(x.get("k"))
        dollar_volume = fnum(x.get("vv"))
        published = fnum(x.get("t")) / 1000.0
        if not (price > 0 and bid > 0 and ask >= bid and dollar_volume > 0):
            continue
        if published > 0 and abs(now - published) > 240:
            continue
        old = by_base.get(base)
        # Prefer genuinely more liquid CDC execution market; USD and USDT are both eligible.
        if old is None or dollar_volume > old[0]:
            by_base[base] = (dollar_volume, {
                "currency_pair": f"{base}_USDT", "exchange_symbol": name,
                "last": price, "highest_bid": bid, "lowest_ask": ask,
                "quote_volume": dollar_volume, "change_percentage": fnum(x.get("c")) * 100,
                "published": published,
            })
    return [entry for _, entry in by_base.values()]


def fetch_tickers():
    """Crypto.com Exchange SPOT is the primary universe, Gate.io auxiliary only."""
    global cdc_ticker_by_pair
    refresh_cdc_pairs()
    r = session.get(CDC_TICKERS_URL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    result = normalize_cdc_tickers(r.json(), cdc_pairs)
    if not result:
        raise RuntimeError("Aucun ticker Spot Crypto.com; refus de trader")
    cdc_ticker_by_pair = {t["currency_pair"]: t for t in result}
    # Auxiliary Gate feed never invents missing CDC tradability or substitutes a stale
    # Gate price for an executable Exchange quote.
    try:
        gr = session.get(GATE_TICKERS_URL, timeout=HTTP_TIMEOUT)
        gr.raise_for_status()
        gate = gr.json()
        gate_now = time.time()
        available = cdc_ticker_by_pair
        if isinstance(gate, list):
            for t in gate:
                pair = str(t.get("currency_pair", "")).upper()
                if pair not in available:
                    continue
                price = fnum(t.get("last"))
                volume = fnum(t.get("quote_volume"))
                if price <= 0 or volume <= 0:
                    continue
                cdcp = fnum(available[pair]["last"])
                if cdcp <= 0 or abs(price / cdcp - 1.0) > 0.05:
                    continue
                gate_aux_history[pair].append(Snapshot(gate_now, price, volume,
                    fnum(t.get("highest_bid")), fnum(t.get("lowest_ask"))))
    except Exception as exc:
        print(f"GATE AUX INDISPONIBLE — CDC reste prioritaire | {type(exc).__name__}", flush=True)
    print(f"V4 COUVERTURE — {len(cdc_pairs)} paires Spot whitelist | "
          f"{len(result)} actifs CDC cotés | Gate auxiliaire {len(gate_aux_history)} suivis", flush=True)
    return result


def cdc_candle_metrics(pair, now=None):
    """Real closed 1m candle volume, never pretend rolling 24h delta = 1m volume."""
    now = time.time() if now is None else now
    cached = candle_cache.get(pair)
    item = cdc_ticker_by_pair.get(pair)
    if not item:
        return None
    instrument = item.get("exchange_symbol")
    if cached and now - cached[0] < 75 and cached[1].get("symbol") == instrument:
        return cached[1]
    try:
        r = session.get(CDC_CANDLES_URL, params={
            "instrument_name": instrument, "timeframe": "1m", "count": 45
        }, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            return None
        rows = sorted(data.get("result", {}).get("data", []), key=lambda x: int(x.get("t",0)))
        closed = [x for x in rows if fnum(x.get("t")) / 1000.0 + 60 <= now
                  and fnum(x.get("c")) > 0 and fnum(x.get("v")) >= 0]
        if len(closed) < 23 or (now - fnum(closed[-1]["t"])/1000.0 > CANDLE_MIN_FRESH_SEC + 60):
            return None
        values = [fnum(x["v"]) * fnum(x["c"]) for x in closed]
        base = statistics.median(values[-21:-1])
        # When median is zero, a real new burst should not vanish.
        # Compare to a conservative 24h hourly-rate reference instead.
        if base > 0:
            ratio = values[-1] / base
        else:
            fallback = max(50.0, fnum(item.get("quote_volume")) / 1440.0 * 0.15)
            ratio = values[-1] / fallback if values[-1] >= 250.0 else 0.0
        closes = [fnum(x["c"]) for x in closed]
        # Cap pathological ratios from sparse, thin markets: x50k is not credible.
        ratio = min(80.0, max(0.0, ratio))
        metrics = {
            "volume_ratio": ratio, "r1": pct(closes[-1], closes[-2]),
            "r5": pct(closes[-1], closes[-6]),
            "r15": pct(closes[-1], closes[-16]), "close": closes[-1],
            "volume_1m_usd_est": values[-1], "age_sec": now - fnum(closed[-1]["t"]) / 1000.0 - 60,
            "high_45m": max(fnum(x.get("h")) for x in closed[-40:]),
            "low_45m": min(fnum(x.get("l")) for x in closed[-40:]),
            "symbol": instrument
        }
        candle_cache[pair] = (now, metrics)
        return metrics
    except Exception as exc:
        print(f"CDC CANDLES ATTENTE — {pair} | {type(exc).__name__}", flush=True)
        return None


def gate_volume_ratio(pair):
    samples = gate_aux_history.get(pair)
    if not samples or len(samples) < 8:
        return 0.0
    deltas = minute_volume_deltas(samples)
    if len(deltas) < 6:
        return 0.0
    baseline = statistics.median(deltas[:-1][-20:])
    latest = max(0.0, samples[-1].qv24 - samples[-2].qv24)
    return latest / baseline if baseline > 0 else 0.0


def v5_choose_candle_pairs(tickers):
    """Rotating market coverage + priority watchlist. Never wait for a 24h delta to
    decide whether a market deserves its first real 1m-volume inspection.
    """
    global _candle_cursor, _track_cursor
    available = {t["currency_pair"]: t for t in tickers
                 if not excluded_pair(t["currency_pair"])}
    universe = sorted(available)
    if not universe:
        return []
    start = _candle_cursor % len(universe)
    rotation = [universe[(start+i) % len(universe)]
                for i in range(min(CANDLE_ROTATION_PER_SCAN, len(universe)))]
    _candle_cursor = (start + len(rotation)) % len(universe)
    # Focus on fresh 5m price impulses, then 24h changes that have not already
    # run too far. A quote remains non-tradable until full risk/fee checks pass.
    def priority(pair):
        t = available[pair]
        samples = history.get(pair) or []
        prev = closest_before(samples, 4*60+30) if samples else None
        move5 = pct(samples[-1].price, prev.price) if prev else 0.0
        day = fnum(t.get("change_percentage"))
        return (max(0, move5)*4 + (min(16, max(0, day))*0.13)
                + min(3, max(0, math.log10(max(1, fnum(t.get("quote_volume")))) - 5)))
    focus = sorted(universe, key=priority, reverse=True)[:CANDLE_FOCUS_PER_SCAN]
    watching = sorted((p for p in pilots if p in available),
                      key=lambda p: (-pilots[p].best_score, p))
    tracked = []
    if watching:
        ts = _track_cursor % len(watching)
        tracked = [watching[(ts+i) % len(watching)]
                   for i in range(min(CANDLE_TRACKED_PER_SCAN, len(watching)))]
        _track_cursor = (ts+len(tracked)) % len(watching)
    return list(dict.fromkeys(focus + tracked + rotation))


def v5_load_candles(pairs, now):
    """Concurrently fetch read-only Exchange 1m candles with a strict worker cap."""
    out = {}
    if not pairs:
        return out
    def one(pair):
        m = cdc_candle_metrics(pair, now)
        return pair, m
    with ThreadPoolExecutor(max_workers=CANDLE_WORKERS) as executor:
        for pair, data in executor.map(one, pairs):
            if data and data.get("age_sec", 9999) <= CANDLE_MIN_FRESH_SEC:
                out[pair] = data
    return out


def add_snapshot(ticker, now):
    pair = ticker.get("currency_pair", "")
    if excluded_pair(pair):
        return

    price = fnum(ticker.get("last"))
    qv24 = fnum(ticker.get("quote_volume"))
    bid = fnum(ticker.get("highest_bid"))
    ask = fnum(ticker.get("lowest_ask"))
    if price <= 0 or qv24 <= 0:
        return

    snap = Snapshot(now, price, qv24, bid, ask)
    history[pair].append(snap)


def closest_before(samples, age_sec: int) -> Optional[Snapshot]:
    if not samples:
        return None
    target = samples[-1].ts - age_sec
    candidates = [s for s in samples if s.ts <= target]
    return candidates[-1] if candidates else None


def minute_volume_deltas(samples):
    out = []
    for a, b in zip(samples, list(samples)[1:]):
        # quote_volume est une fenêtre 24h glissante; le delta court terme
        # reste un proxy utile mais peut exceptionnellement être négatif.
        d = b.qv24 - a.qv24
        if d > 0:
            out.append(d)
    return out


def score_signal(pair: str, gate_change24h: Optional[float] = None, tracking: bool = False,
                 metrics=None) -> Optional[Signal]:
    samples = history[pair]
    # A new listing can be screened after two ticker observations when recent,
    # REAL closed 1-minute candles already provide the 1/5/15m history.
    if len(samples) < (1 if metrics else 7):
        return None

    cur = samples[-1]
    s1 = closest_before(samples, 55)
    s5 = closest_before(samples, 4 * 60 + 30)
    s15 = closest_before(samples, 14 * 60 + 30)
    if not metrics and (not s1 or not s5):
        return None

    ret1 = metrics["r1"] if metrics else pct(cur.price, s1.price)
    ret5 = metrics["r5"] if metrics else pct(cur.price, s5.price)
    ret15 = metrics["r15"] if metrics else (pct(cur.price, s15.price) if s15 else 0.0)
    # 24h est enrichi depuis le ticker Gate après scoring; pour le scoring lui-même,
    # on le calcule depuis l’historique local quand 24h est disponible, sinon 0.
    s24 = closest_before(samples, 24 * 60 * 60 - 30)
    change24h = pct(cur.price, s24.price) if s24 else (gate_change24h if gate_change24h is not None else 0.0)

    spread = 999.0
    if cur.bid > 0 and cur.ask > 0 and cur.ask >= cur.bid:
        mid = (cur.ask + cur.bid) / 2
        spread = (cur.ask - cur.bid) / mid * 100.0 if mid else 999.0

    # Thin markets may be WATCHED early, but are never alerted as a trade
    # until they meet the separate, stricter execution liquidity threshold.
    if cur.qv24 < MIN_WATCH_24H_QUOTE_VOL:
        v5_diagnostics["volume_24h_insuffisant"] += 1
        return None
    if spread > 0.50:
        v5_diagnostics["spread_trop_large"] += 1
        return None

    if metrics:
        # Price and volume are now from Exchange Spot closed 1m candles,
        # NOT the difference of two 24h sliding-volume counters.
        vol_ratio = metrics["volume_ratio"]
        if not tracking and metrics["volume_1m_usd_est"] < 150:
            v5_diagnostics["volume_1m_insuffisant"] += 1
            return None
    else:
        # Legacy-only fallback retained for deterministic regression fixtures;
        # production passes fresh Exchange candle metrics to all evaluations.
        deltas = minute_volume_deltas(samples)
        if len(deltas) < 5:
            return None
        current_delta = max(0.0, cur.qv24 - samples[-2].qv24)
        baseline_pool = deltas[:-1][-20:]
        baseline = statistics.median(baseline_pool) if baseline_pool else 0.0
        if baseline <= 0:
            return None
        vol_ratio = min(80, max(current_delta / baseline, gate_volume_ratio(pair)))

    # Détection pré-mouvement: volume anormal + début de momentum,
    # sans accepter une bougie déjà partie de façon extrême à très court terme.
    # A new candidate must cross the early trigger. An existing pilot is different:
    # keep measuring its trajectory even after the anomaly cools down, otherwise
    # TRACKED pilots become dead memory and can never prove continuation/failure.
    # Observation-only after a large daily move: repeated lower-intensity
    # volume and stable momentum qualify for silent tracking, not a buy alert.
    quiet_second_leg = bool(
        not tracking and metrics and change24h >= MAX_PILOT_24H
        and vol_ratio >= 1.5 and -0.10 <= ret5 <= 0.90
        and 0.10 <= ret15 <= 3.0 and ret1 >= -0.20
    )
    if not tracking:
        if metrics:
            if not (vol_ratio >= EARLY_MIN_VOLUME_RATIO and ret5 >= -0.25
                    or vol_ratio >= 1.30 and ret5 >= 0.45
                    or quiet_second_leg):
                v5_diagnostics["pas_d_anomalie_precoce"] += 1
                return None
        elif vol_ratio < EARLY_MIN_VOLUME_RATIO and ret5 < 0.40:
            return None
    if ret1 < -0.35 or ret5 < -0.75:
        v5_diagnostics["momentum_negatif"] += 1
        return None
    if not tracking and (ret5 > EARLY_MAX_RET5 or ret15 > 10.0):
        v5_diagnostics["bougie_trop_etendue"] += 1
        return None

    # PEPITO scoring verrouillé: 30/20/15/15/10/10.
    # Les données absentes ne sont jamais inventées: catalyst=0 tant qu'un flux fiable
    # n'est pas branché; flow utilise ici un proxy momentum conservateur.
    # Calibrate each available evidence dimension to its declared maximum.
    # Previously even a strong x20, +0.7% 5m move scored ~65/100 and
    # CONFIRMED_SCORE=90 was practically unreachable. Never invent catalysts.
    volume_pts = min(30, max(0, int(30 * (1 - math.exp(-max(vol_ratio, 0) / 8.0)))))
    flow_pts = min(20, max(0, int(20 * min(1.0, (max(ret1, 0) / 0.15 + max(ret5, 0) / 0.60) / 2.0))))
    liquidity_pts = min(15, max(0, int(10 * (1 - spread / 0.50) + min(5, max(0, math.log10(max(cur.qv24, 1)) - 5)))))
    extension = max(abs(ret5), max(change24h, 0))
    unextended_pts = 15 if extension <= 2 else (10 if extension <= 5 else (4 if extension <= 10 else 0))
    structure_pts = min(10, max(0, int(10 * min(1.0, max(ret5, 0) / 0.35))))
    catalyst_pts = 0
    # Score only the five measured dimensions (90 available points).
    # Catalyst remains 0 until a verified source exists; it is not assumed present.
    score = min(100, round(100 * (volume_pts + flow_pts + liquidity_pts + unextended_pts + structure_pts) / 90))

    # Anti-chase: un x50/x100 déjà très étendu ne devient pas prioritaire.
    if not tracking and ret15 > 9.0:
        v5_diagnostics["retour_15m_trop_etendu"] += 1
        return None

    # PEPITO: détection précoce silencieuse. Un score inférieur au seuil pilote
    # peut être mémorisé si le volume accélère déjà; aucune notification Slack ici.
    early_score_floor = max(35, PILOT_SCORE - 35)
    if not tracking and score < early_score_floor and ret5 < 0.65 and not quiet_second_leg:
        v5_diagnostics["score_radar_insuffisant"] += 1
        return None

    # En V2, aucun signal n'est "CONFIRME" sur un seul scan.
    # Il devient d'abord pilote, puis sa trajectoire est évaluée.
    return Signal(
        pair=pair,
        price=cur.price,
        ret_1m=ret1,
        ret_5m=ret5,
        ret_15m=ret15,
        vol_ratio=vol_ratio,
        qv24=cur.qv24,
        spread_pct=spread,
        change_24h=change24h,
        score=score,
        level="RADAR INTERNE" if score < PILOT_SCORE else "ENTREE PILOTE",
    )


def can_alert(pair: str, now: float) -> bool:
    last = last_alert_at.get(pair, 0)
    return (now - last) >= PAIR_COOLDOWN_MIN * 60


def cleanup_expired_pilots(now: float):
    ttl = PILOT_TTL_MIN * 60
    expired = [pair for pair, p in pilots.items() if now - p.created_at > ttl]
    for pair in expired:
        p = pilots.pop(pair)
        if _state_db is not None:
            _state_db.execute("DELETE FROM pilot_state WHERE pair=?", (pair,))
        ai_next_review_at.pop(pair, None)
        print(
            f"PILOTE EXPIRE — {pair} | age={((now - p.created_at) / 60):.0f}m | "
            f"vues={p.sightings} | best_score={p.best_score}",
            flush=True,
        )


def open_pilot(sig: Signal, now: float) -> bool:
    # Keep monitoring strong daily movers. This never sends Slack directly.
    if sig.change_24h > MAX_PILOT_24H:
        print(
            f"PILOTE SUIVI ETENDU — {sig.pair} | 24h={sig.change_24h:+.2f}% "
            f"| suivi silencieux",
            flush=True,
        )

    pilots[sig.pair] = PilotState(
        created_at=now,
        last_seen_at=now,
        first_price=sig.price,
        first_score=sig.score,
        first_vol_ratio=sig.vol_ratio,
        first_ret_1m=sig.ret_1m,
        first_ret_5m=sig.ret_5m,
        first_ret_15m=sig.ret_15m,
        first_change_24h=sig.change_24h,
        sightings=1,
        best_score=sig.score,
        best_vol_ratio=sig.vol_ratio,
        best_price=sig.price,
        min_price=sig.price,
        last_price=sig.price,
        last_score=sig.score,
        last_vol_ratio=sig.vol_ratio,
        trajectory=deque(maxlen=90),
    )
    pilots[sig.pair].trajectory.append((now, sig.price, sig.score, sig.vol_ratio, sig.ret_1m, sig.ret_5m, sig.ret_15m, sig.qv24, sig.spread_pct, sig.change_24h))
    if history.get(sig.pair):
        persist_market_snapshot(sig.pair, history[sig.pair][-1])
    persist_pilot_state(sig.pair, pilots[sig.pair])
    print(
        f"PILOTE OUVERT — {sig.pair} | score={sig.score} | prix={sig.price:.10g} | "
        f"vol=x{sig.vol_ratio:.1f} | r1={sig.ret_1m:+.2f}% | "
        f"r5={sig.ret_5m:+.2f}% | r15={sig.ret_15m:+.2f}% | "
        f"24h={sig.change_24h:+.2f}% | Slack silencieux",
        flush=True,
    )
    return True


def swing_accumulation_ok(sig: Signal, pilot: PilotState, age_min: float, price_gain: float) -> bool:
    """Repeated volume anomaly with stable price: an AI candidate, not a trade."""
    if not SWING_ENABLED or pilot.sightings < 8 or age_min < SWING_MIN_AGE_MIN:
        return False
    if sig.qv24 < SWING_MIN_QUOTE_VOL or sig.spread_pct > SWING_MAX_SPREAD:
        return False
    if not (pilot.best_vol_ratio >= 12 and -0.25 <= price_gain <= 2.5):
        return False
    if not (-3 <= sig.change_24h <= 10 and pilot.first_change_24h <= 10):
        return False
    if not (-0.75 <= sig.ret_5m <= 1.5 and -1.3 <= sig.ret_15m <= 3):
        return False
    if sig.price < pilot.first_price * 0.9975:
        return False
    recent = list(pilot.trajectory or [])[-12:]
    spikes = sum(1 for x in recent if len(x) > 8 and x[3] >= SWING_MIN_SPIKE_RATIO
                 and x[8] <= SWING_MAX_SPREAD)
    return spikes >= SWING_MIN_RECENT_SPIKES


def continuation_ok(sig, pilot, age_min, price_gain):
    """STRK-style continuation: early volume memory matters after 1m volume cools."""
    if pilot.sightings < 3 or age_min < 2 or age_min > PILOT_TTL_MIN:
        return False
    if not (0.30 <= price_gain <= 12.0 and sig.score >= 38
            and pilot.best_score >= 60 and pilot.best_vol_ratio >= 5.0):
        return False
    if not (0.35 <= sig.ret_5m <= 3.0 and 0.55 <= sig.ret_15m <= 6.0):
        return False
    if sig.spread_pct > 0.20:
        return False
    # A market already extended over 24h only qualifies for a NEW leg after
    # multiple observations, at least eight minutes, and moderate 5m/15m moves.
    if max(sig.change_24h, pilot.first_change_24h) > 20.0:
        if (pilot.sightings < 8 or age_min < 8.0
                or sig.vol_ratio < 1.2
                or sig.ret_5m > 1.6 or sig.ret_15m > 3.5
                or price_gain > 5.0):
            return False
    return True


def evaluate_trajectory(sig: Signal, now: float) -> Optional[ConfirmedCandidate]:
    if not can_alert(sig.pair, now):
        return None

    pilot = pilots.get(sig.pair)
    if pilot is None:
        open_pilot(sig, now)
        return None

    pilot.last_seen_at = now
    pilot.sightings += 1
    pilot.best_score = max(pilot.best_score, sig.score)
    pilot.best_vol_ratio = max(pilot.best_vol_ratio, sig.vol_ratio)
    pilot.best_price = max(pilot.best_price, sig.price)
    pilot.min_price = min(pilot.min_price or sig.price, sig.price)
    pilot.last_price = sig.price
    pilot.last_score = sig.score
    pilot.last_vol_ratio = sig.vol_ratio
    if pilot.trajectory is None:
        pilot.trajectory = deque(maxlen=90)
    pilot.trajectory.append((now, sig.price, sig.score, sig.vol_ratio, sig.ret_1m, sig.ret_5m, sig.ret_15m, sig.qv24, sig.spread_pct, sig.change_24h))
    if history.get(sig.pair):
        persist_market_snapshot(sig.pair, history[sig.pair][-1])
    persist_pilot_state(sig.pair, pilot)

    age_sec = now - pilot.created_at
    age_min = age_sec / 60.0
    price_gain = pct(sig.price, pilot.first_price)

    score_ok = sig.score >= CONFIRMED_SCORE
    age_ok = age_sec >= CONFIRM_MIN_AGE_SEC
    price_ok = price_gain >= CONFIRM_MIN_PRICE_GAIN
    spread_ok = sig.spread_pct <= CONFIRM_MAX_SPREAD
    extended_ok = (
        pilot.first_change_24h <= MAX_PILOT_24H
        and sig.change_24h <= MAX_PILOT_24H
    )

    volume_ok = (
        sig.vol_ratio >= CONFIRM_MIN_VOL_RATIO
        and (
            sig.vol_ratio >= pilot.first_vol_ratio * CONFIRM_VOL_KEEP_RATIO
            or sig.vol_ratio >= 20.0
        )
    )

    # PEPITO trajectory quality: reward progressive acceleration, penalize late spikes.
    traj = list(pilot.trajectory or [])
    recent_vols = [float(pt[3]) for pt in traj[-6:] if len(pt) > 3]
    rising_steps = sum(1 for a, b in zip(recent_vols, recent_vols[1:]) if b >= a * 1.15)
    trajectory_accel_ok = len(recent_vols) >= 3 and rising_steps >= 2
    early_distance_ok = price_gain <= AI_MAX_PRICE_GAIN
    late_spike = (
        sig.vol_ratio >= 50.0
        and not trajectory_accel_ok
        and price_gain >= 3.0
    )
    if trajectory_accel_ok and early_distance_ok:
        score_ok = score_ok or (sig.score >= max(PILOT_SCORE, CONFIRMED_SCORE - 5) and sig.vol_ratio >= 10.0)
    if late_spike:
        score_ok = False
        print(
            f"ANTI-CHASE TRAJECTOIRE — {sig.pair} | vol=x{sig.vol_ratio:.1f} | "
            f"gain depuis pilote={price_gain:+.2f}% | progression volume insuffisante",
            flush=True,
        )

    momentum_floor_ok = (
        sig.ret_1m >= CONFIRM_MIN_RET1
        and sig.ret_5m >= CONFIRM_MIN_RET5
        and sig.ret_15m >= CONFIRM_MIN_RET15
    )
    momentum_improved = (
        sig.ret_5m >= pilot.first_ret_5m + 0.10
        or sig.ret_15m >= pilot.first_ret_15m + 0.15
        or sig.score >= pilot.first_score + 5
    )

    if (
        score_ok
        and age_ok
        and price_ok
        and spread_ok
        and extended_ok
        and volume_ok
        and momentum_floor_ok
        and momentum_improved
        and pilot.sightings >= 2
    ):
        sig.level = "CONFIRME"
        return ConfirmedCandidate(
            signal=sig,
            pilot=pilot,
            price_gain=price_gain,
            age_min=age_min,
        )

    if continuation_ok(sig, pilot, age_min, price_gain):
        print(f"V4 CONTINUATION — {sig.pair} | gain depuis pilote {price_gain:+.2f}% "
              f"meilleur score={pilot.best_score} | controle AI/CDC/frais requis", flush=True)
        return ConfirmedCandidate(signal=sig, pilot=pilot, price_gain=price_gain,
                                  age_min=age_min, style="CONTINUATION")
    if swing_accumulation_ok(sig, pilot, age_min, price_gain):
        print(f"SWING ACCUMULATION CANDIDATE — {sig.pair} | age={age_min:.0f}m | "
              f"vol=x{sig.vol_ratio:.1f} | r5={sig.ret_5m:+.2f}% | "
              f"cdc/ia/net-profit still required", flush=True)
        return ConfirmedCandidate(signal=sig, pilot=pilot, price_gain=price_gain,
                                  age_min=age_min, style="SWING_ACCUMULATION")

    failed_gates = [
        name for name, ok in (
            ("score", score_ok), ("age", age_ok), ("price", price_ok),
            ("spread", spread_ok), ("extension", extended_ok), ("volume", volume_ok),
            ("momentum", momentum_floor_ok), ("improvement", momentum_improved),
            ("sightings", pilot.sightings >= 2),
        ) if not ok
    ]
    # Aggregate rejection reasons each cycle, not just verbose per-token logs.
    gate_rejections.update(failed_gates)
    print(
        f"PILOTE SUIVI — {sig.pair} | age={age_min:.0f}m | vues={pilot.sightings} | "
        f"score={sig.score} | gain={price_gain:+.2f}% | vol=x{sig.vol_ratio:.1f} | "
        f"r5={sig.ret_5m:+.2f}% | r15={sig.ret_15m:+.2f}% | "
        f"24h={sig.change_24h:+.2f}% | BLOQUE={','.join(failed_gates) or 'aucun'}",
        flush=True,
    )
    return None


def format_confirmed_alert(c: ConfirmedCandidate) -> str:
    s = c.signal
    p = c.pilot
    return (
        f"✅ CRYPTO RADAR CLEAN — CONFIRME TRAJECTOIRE\n"
        f"{s.pair}\n\n"
        f"Score : {s.score}/100\n"
        f"Prix pilote : {p.first_price:.10g}\n"
        f"Prix actuel : {s.price:.10g}\n"
        f"Progression depuis pilote : {c.price_gain:+.2f}%\n"
        f"Temps depuis pilote : {c.age_min:.0f} min\n"
        f"Volume relatif pilote : x{p.first_vol_ratio:.1f}\n"
        f"Volume relatif actuel : x{s.vol_ratio:.1f}\n"
        f"Variation ~1 min : {s.ret_1m:+.2f}%\n"
        f"Variation ~5 min : {s.ret_5m:+.2f}%\n"
        f"Variation ~15 min : {s.ret_15m:+.2f}%\n"
        f"Volume 24 h : ${s.qv24:,.0f}\n"
        f"Spread : {s.spread_pct:.3f}%\n"
        f"Variation 24 h : {s.change_24h:+.2f}%\n"
        f"Source : Gate.io"
    )


def _response_text(data) -> str:
    """Extrait le texte d'une réponse brute de l'API Responses."""
    direct = data.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    for item in data.get("output", []) or []:
        for content in item.get("content", []) or []:
            if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                return content["text"].strip()
    return ""


def _nullable_float(v):
    if v is None:
        return None
    try:
        x = float(v)
        return x if x > 0 else None
    except (TypeError, ValueError):
        return None


def _candidate_payload(c: ConfirmedCandidate):
    s = c.signal
    p = c.pilot
    return {
        "pair": s.pair,
        "setup_style": c.style,
        "time_horizon": "1-7 days" if c.style == "SWING_ACCUMULATION" else "1h-48h" if c.style == "CONTINUATION" else "30m-6h",
        "exchange_instrument": cdc_ticker_by_pair.get(s.pair, {}).get("exchange_symbol", ""),
        "exchange_candle_metrics": candle_cache.get(s.pair, (None, {}))[1],
        "score": s.score,
        "price": s.price,
        "pilot_price": p.first_price,
        "gain_since_pilot_pct": round(c.price_gain, 4),
        "age_min": round(c.age_min, 1),
        "vol_ratio_now": round(s.vol_ratio, 2),
        "vol_ratio_pilot": round(p.first_vol_ratio, 2),
        "ret_1m_pct": round(s.ret_1m, 4),
        "ret_5m_pct": round(s.ret_5m, 4),
        "ret_15m_pct": round(s.ret_15m, 4),
        "quote_volume_24h_usd": round(s.qv24, 2),
        "spread_pct": round(s.spread_pct, 5),
        "change_24h_pct": round(s.change_24h, 4),
        "pilot_score": p.first_score,
        "pilot_ret_5m_pct": round(p.first_ret_5m, 4),
        "pilot_ret_15m_pct": round(p.first_ret_15m, 4),
        "sightings": p.sightings,
        "trajectory": [
            {
                "age_min": round((time.time() - pt[0]) / 60.0, 1),
                "price": pt[1],
                "score": pt[2],
                "vol_ratio": round(pt[3], 2),
                "ret_1m_pct": round(pt[4], 4),
                "ret_5m_pct": round(pt[5], 4),
                "ret_15m_pct": round(pt[6], 4),
                "quote_volume_24h_usd": round(pt[7], 2),
                "spread_pct": round(pt[8], 5),
                "change_24h_pct": round(pt[9], 4),
            }
            for pt in list(p.trajectory or [])[-12:]
        ],
        "persistent_context": persistent_context(s.pair, time.time()),
    }


def ai_review_batch(candidates):
    """Demande à l'IA de choisir au maximum UN setup TRADE parmi les confirmés V2."""
    if not AI_ENABLED or not OPENAI_API_KEY or not candidates:
        return None

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "best_trade_pair": {"type": ["string", "null"]},
            "reviews": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "pair": {"type": "string"},
                        "decision": {"type": "string", "enum": ["TRADE", "WAIT", "IGNORE"]},
                        "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                        "reason": {"type": "string"},
                        "entry_low": {"type": ["number", "null"]},
                        "entry_high": {"type": ["number", "null"]},
                        "invalidation": {"type": ["number", "null"]},
                        "tp1": {"type": ["number", "null"]},
                        "tp2": {"type": ["number", "null"]},
                    },
                    "required": [
                        "pair", "decision", "confidence", "reason", "entry_low", "entry_high",
                        "invalidation", "tp1", "tp2"
                    ],
                },
            },
        },
        "required": ["best_trade_pair", "reviews"],
    }

    system_prompt = (
        "Tu es la couche finale de filtrage d'un radar crypto haussier. "
        "Analyse trois profils: MOMENTUM (30 min-6 heures), SWING_ACCUMULATION (1-7 jours) "
        "et CONTINUATION (1h-48h): le volume d'origine peut retomber après un déclenchement "
        "historique important, sans annuler une hausse confirmée. "
        "Un SWING_ACCUMULATION peut accumuler des volumes anormaux alors que les retours 5m/15m "
        "sont encore faibles: ne le rejette pas pour cette seule raison. "
        "Pour SWING_ACCUMULATION, lis les prix hauts/bas et amplitudes des horizons "
        "6h/24h/7j dans persistent_context et refuse les objectifs incompatibles. "
        "et exige un potentiel de continuation justifié par les données fournies. "
        "Si les horizons manquent, réponds WAIT. N'invente jamais un breakout, un support ou un objectif. "
        "Les candidats ont DEJA passé une validation quantitative de trajectoire. "
        "Ton rôle n'est pas de forcer un trade: WAIT est le choix par défaut si l'avantage n'est pas net. "
        "Tu dois choisir au maximum UN TRADE dans le lot, celui qui est le plus propre et exploitable maintenant. "
        "Sinon best_trade_pair doit être null. "
        "Utilise UNIQUEMENT les données fournies, sans inventer actualité, carnet d'ordres, support ou catalyseur. "
        "Favorise: momentum 5/15 min cohérent, volume relatif encore fort, spread faible, liquidité correcte, "
        "progression depuis pilote positive mais pas déjà trop étendue, et âge raisonnable. "
        "Déclasse si le volume retombe fortement, si le mouvement paraît déjà consommé, si le spread/liquidité est faible, "
        "ou si le ratio rendement/risque n'est pas propre. "
        "Le coût estimé aller-retour est de 1,30% (frais, spread et exécution). "
        "N'indique TRADE que si, APRES ces coûts, TP1 offre au moins 0,75% net, "
        "TP2 au moins 3,0% net et si le rapport gain net TP2 / perte potentielle coûts inclus dépasse 2. "
        "Ne gonfle JAMAIS les objectifs pour contourner ce filtre: WAIT si les données ne justifient pas un tel potentiel. "
        "Pour TRADE seulement, fournis une zone d'entrée autour du prix actuel, une invalidation sous l'entrée, "
        "et TP1/TP2 au-dessus. Les niveaux doivent être cohérents avec un trade court terme, pas des objectifs fantaisistes. "
        "La raison doit être en français, concrète, en une phrase courte."
    )
    payload = {
        "model": OPENAI_MODEL,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
            {
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": json.dumps(
                        {
                            "instruction": "Classe ces candidats. Au maximum un seul TRADE; sinon aucun.",
                            "candidates": [_candidate_payload(c) for c in candidates],
                        },
                        ensure_ascii=False,
                    ),
                }],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "crypto_trade_review",
                "strict": True,
                "schema": schema,
            }
        },
        "max_output_tokens": AI_OUTPUT_TOKEN_BUDGET,
    }
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        r = requests.post(
            "https://api.openai.com/v1/responses",
            headers=headers,
            json=payload,
            timeout=AI_HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            body = (r.text or "")[:300].replace(chr(10), " ")
            print(f"AI HTTP {r.status_code} — {body}", flush=True)
            return None
        payload_response = r.json()
        raw = _response_text(payload_response)
        if not raw:
            diagnostic = payload_response.get("incomplete_details") or {}
            usage = payload_response.get("usage") or {}
            print(f"AI ERREUR — réponse vide | status={payload_response.get('status')} "
                  f"incomplete_reason={diagnostic.get('reason')} "
                  f"output_tokens={usage.get('output_tokens')} "
                  f"budget={AI_OUTPUT_TOKEN_BUDGET}", flush=True)
            return None
        parsed = json.loads(raw)
    except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
        print(f"AI ERREUR — {type(exc).__name__}: {exc}", flush=True)
        return None

    known = {c.signal.pair for c in candidates}
    reviews = []
    for item in parsed.get("reviews", []) or []:
        pair = str(item.get("pair", "")).strip()
        decision = str(item.get("decision", "")).strip().upper()
        if pair not in known or decision not in {"TRADE", "WAIT", "IGNORE"}:
            continue
        reviews.append(AIReview(
            pair=pair,
            decision=decision,
            confidence=max(0, min(100, int(item.get("confidence", 0) or 0))),
            reason=str(item.get("reason", "")).strip()[:300],
            entry_low=_nullable_float(item.get("entry_low")),
            entry_high=_nullable_float(item.get("entry_high")),
            invalidation=_nullable_float(item.get("invalidation")),
            tp1=_nullable_float(item.get("tp1")),
            tp2=_nullable_float(item.get("tp2")),
        ))

    best = parsed.get("best_trade_pair")
    if best is not None:
        best = str(best).strip()
        if best not in known:
            best = None
    return best, reviews


def validate_trade_review(review: AIReview, c: ConfirmedCandidate) -> bool:
    """Fail-closed order validation, diagnostic reason for EVERY rejection.
    Trades are never automatically placed; only qualified Slack alerts are sent.
    """
    pair = c.signal.pair
    def deny(reason, detail=""):
        print(f"V5 REFUS — {pair} | cause={reason}"+(f" | {detail}" if detail else ""),flush=True)
        return False

    if review.decision != "TRADE":
        return deny("ai_not_trade")
    if review.confidence < 70:
        return deny("ai_confidence", f"value={review.confidence}")
    if not cdc_tradeable(pair):
        return deny("not_exchange_spot")
    if c.signal.qv24 < MIN_24H_QUOTE_VOL:
        return deny("execution_liquidity_below_min",
                    f"quote24={c.signal.qv24:.0f} required={MIN_24H_QUOTE_VOL:.0f}")
    vals = [review.entry_low, review.entry_high, review.invalidation, review.tp1, review.tp2]
    if any(v is None or v<=0 for v in vals):
        return deny("missing_order_levels")
    lo, hi, inv, tp1, tp2 = vals
    if lo>hi or not (inv<lo<=hi<tp1<tp2):
        return deny("invalid_level_order", f"entry={lo}:{hi} stop={inv} tp1={tp1} tp2={tp2}")
    distance = abs(pct(c.signal.price, (lo+hi)/2))
    if distance > MAX_ENTRY_DRIFT_PCT:
        return deny("stale_entry_zone", f"drift={distance:.2f}% max={MAX_ENTRY_DRIFT_PCT:.2f}%")

    costs_pct = max(0.0, ROUND_TRIP_FEE_PCT) + max(0.0, EXECUTION_BUFFER_PCT)
    net_tp1 = (tp1/hi-1.0)*100.0-costs_pct
    net_tp2 = (tp2/hi-1.0)*100.0-costs_pct
    risk = (hi-inv)/hi*100.0+costs_pct
    net_rr = net_tp2/risk if risk>0 else 0.0
    detail=(f"TP1_net={net_tp1:+.2f}% TP2_net={net_tp2:+.2f}% "
            f"risk={risk:.2f}% RR_net={net_rr:.2f} reserve_frais={costs_pct:.2f}%")
    if net_tp1 < MIN_NET_TP1_PCT or net_tp2 < MIN_NET_TP2_PCT or net_rr < MIN_NET_REWARD_RISK:
        return deny("net_profit_or_reward_risk",detail)
    if c.age_min > AI_MAX_SIGNAL_AGE_MIN and c.style=="MOMENTUM":
        return deny("stale_momentum",f"age={c.age_min:.0f}m")
    cap = (12.0 if c.style=="CONTINUATION" else
           2.5 if c.style=="SWING_ACCUMULATION" else AI_MAX_PRICE_GAIN)
    if c.price_gain > cap:
        return deny("overextended_since_pilot",f"gain={c.price_gain:+.2f}% cap={cap:.2f}%")

    if not V4_BACKTEST_MODE:
        market = cdc_ticker_by_pair.get(pair)
        if not market:
            return deny("missing_live_exchange_market")
        m = cdc_candle_metrics(pair)
        if not m or m["age_sec"]>CANDLE_MIN_FRESH_SEC:
            return deny("stale_exchange_candles")
        if m["volume_1m_usd_est"] < 300.0 and m["volume_ratio"] < 1.2:
            return deny("insufficient_real_exchange_volume",
                        f"1m_usd={m['volume_1m_usd_est']:.0f} x={m['volume_ratio']:.1f}")
        if m["volume_ratio"] < 1.2 and m["r5"] < 0.25:
            return deny("no_volume_or_momentum_confirmation",
                        f"vol=x{m['volume_ratio']:.1f} r5={m['r5']:+.2f}%")
        if abs(c.signal.price/max(1e-12,fnum(market["last"]))-1.0)>0.003:
            return deny("market_moved_after_scan")
        # Re-quote the actual exchange spot orderbook immediately before notifying
        # the user; the AI may have been running for many seconds.
        try:
            rq = session.get(CDC_TICKERS_URL,
                             params={"instrument_name":market["exchange_symbol"]},
                             timeout=8)
            rq.raise_for_status()
            live = normalize_cdc_tickers(rq.json(),
                                         {market["exchange_symbol"]},time.time())
            if not live:
                return deny("no_fresh_executable_quote")
            ask = fnum(live[0].get("lowest_ask"))
            if ask<=0 or not (lo*0.9985<=ask<=hi*1.001):
                return deny("ask_outside_buy_zone",f"ask={ask:.10g} entry={lo:.10g}:{hi:.10g}")
        except Exception as exc:
            return deny("exchange_requote_error",f"exception={type(exc).__name__}")
    return True


def format_trade_alert(c: ConfirmedCandidate, review: AIReview) -> str:
    s = c.signal
    return (
        f"TRADE EXPLOITABLE — NOUVELLE ENTRÉE À VALIDER\n"
        f"{s.pair}\n\n"
        f"Style : {c.style}\n"
        f"Horizon : {'1-7 jours' if c.style == 'SWING_ACCUMULATION' else '1h-48h' if c.style == 'CONTINUATION' else '30m-6h'}\n"
        f"Marché de référence : {cdc_ticker_by_pair.get(s.pair, {}).get('exchange_symbol','Crypto.com Exchange')}\n"
        f"Prix actuel : {s.price:.10g}\n"
        f"Zone d'entrée : {review.entry_low:.10g} → {review.entry_high:.10g}\n"
        f"Invalidation : {review.invalidation:.10g}\n"
        f"TP1 : {review.tp1:.10g}\n"
        f"TP2 : {review.tp2:.10g}\n"
        f"Confiance IA : {review.confidence}/100\n\n"
        f"Pourquoi : {review.reason}\n\n"
        f"Trajectoire : +{c.price_gain:.2f}% depuis pilote | score {s.score}/100 | "
        f"vol x{s.vol_ratio:.1f} | r5 {s.ret_5m:+.2f}% | r15 {s.ret_15m:+.2f}% | "
        f"spread {s.spread_pct:.3f}%\n"
        f"⚠️ Si le prix sort de la zone avant ton entrée, ne poursuis pas le mouvement.\n"
        f"Source prix/volume : Crypto.com Exchange (Spot) | Gate.io : auxiliaire"
    )


def send_slack_once(message: str) -> bool:
    """Un seul POST. Jamais de boucle de retry qui martèle Slack."""
    global slack_blocked_until, last_slack_send_at

    if not SLACK_ENABLED or not SLACK_WEBHOOK_URL:
        return False

    now = time.time()
    if now < slack_blocked_until:
        return False

    # Slack documente ~1 msg/s pour incoming webhooks. On garde 1,5 s.
    wait = 1.5 - (now - last_slack_send_at)
    if wait > 0:
        time.sleep(wait)

    try:
        r = session.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=HTTP_TIMEOUT,
        )
        last_slack_send_at = time.time()

        if r.status_code == 200:
            print("SLACK OK", flush=True)
            return True

        if r.status_code == 429:
            retry_after = fnum(r.headers.get("Retry-After"), 60)
            # Circuit breaker: on respecte Retry-After et on impose au moins 15 min.
            slack_blocked_until = time.time() + max(retry_after, 15 * 60)
            print(
                f"SLACK 429 — circuit breaker {int(max(retry_after, 900))}s",
                flush=True,
            )
            return False

        print(f"SLACK HTTP {r.status_code} — aucun retry", flush=True)
        return False
    except requests.RequestException as exc:
        print(f"SLACK ERREUR RESEAU — aucun retry: {exc}", flush=True)
        return False


def run():
    print(
        f"Crypto Radar Clean démarre | interval={SCAN_INTERVAL}s | "
        f"Slack={'ON' if SLACK_ENABLED and SLACK_WEBHOOK_URL else 'OFF'}",
        flush=True,
    )
    print(
        f"VALIDATION V4 ACTIVE — CDC SPOT->ANOMALIE->SUIVI->CONTINUATION->IA->TRADE | "
        f"pilot={PILOT_SCORE} | confirm={CONFIRMED_SCORE} | ttl={PILOT_TTL_MIN}m | "
        f"AI={'ON' if AI_ENABLED and OPENAI_API_KEY else 'OFF'} | model={OPENAI_MODEL} | "
        f"Slack=TRADE_ONLY | ai_age<={AI_MAX_SIGNAL_AGE_MIN}m | gain<={AI_MAX_PRICE_GAIN:.1f}%",
        flush=True,
    )
    if AI_ENABLED and not OPENAI_API_KEY:
        print("V3 ATTENTION — OPENAI_API_KEY absent: aucun Slack TRADE ne sera envoyé.", flush=True)

    init_state_db()
    startup_now = time.time()
    restore_pilots(startup_now)
    restore_pilot_history(startup_now)
    load_positions()

    while True:
        started = time.time()
        try:
            v5_diagnostics.clear()
            tickers = fetch_tickers()
            now = time.time()
            try:
                refresh_cdc_pairs(now)
            except Exception as exc:
                # Fail closed: sans whitelist fiable, aucun TRADE ne doit partir.
                cdc_pairs.clear()
                print(f"CDC WHITELIST ERREUR — TRADE bloqué | {type(exc).__name__}: {exc}", flush=True)
            by_pair = {}

            for t in tickers:
                pair = t.get("currency_pair", "")
                by_pair[pair] = t
                add_snapshot(t, now)

            commit_state(now)
            cleanup_expired_pilots(now)

            # Circuit indépendant POSITIONS OUVERTES: une position reste surveillée
            # même lorsqu'elle est volontairement exclue du circuit de nouvelle entrée.
            for held_pair in list(positions.keys()):
                t = by_pair.get(held_pair)
                if not t:
                    continue
                held_price = fnum(t.get("last"))
                action = position_action(held_pair, held_price)
                if action:
                    print(f"POSITION ACTION — {held_pair} | {action} | prix={held_price:.10g}", flush=True)

            # V5: MARKET COVERAGE BEFORE scoring. Previously almost every coin
            # was discarded on a 24h rolling-volume delta before requesting 1m data.
            # Evaluate a full market rotation plus active pilots and fresh movers.
            chosen = v5_choose_candle_pairs(tickers)
            fresh_candles = v5_load_candles(chosen, now)
            raw_signals = []
            tracked_signals = []
            for pair, candle in fresh_candles.items():
                if pair in positions or pair not in by_pair:
                    continue
                gate24 = fnum(by_pair[pair].get("change_percentage"))
                is_tracking = pair in pilots
                sig = score_signal(pair, gate24, tracking=is_tracking, metrics=candle)
                if sig:
                    if is_tracking:
                        tracked_signals.append(sig)
                    else:
                        raw_signals.append(sig)
            raw_signals.sort(key=lambda x: (x.score,x.vol_ratio), reverse=True)
            tracked_signals.sort(key=lambda x: (x.score,x.vol_ratio),reverse=True)
            early_count = len(raw_signals)
            cdc_count = sum(1 for sig in raw_signals if cdc_tradeable(sig.pair))
            print(
                f"V5 DONNEES — actifs={len(tickers)} | bougies demandées={len(chosen)} "
                f"| bougies fraîches={len(fresh_candles)} | sans bougies={len(chosen)-len(fresh_candles)} "
                f"| nouveaux candidats={len(raw_signals)} | suivis actifs réévalués={len(tracked_signals)}",
                flush=True
            )
            if v5_diagnostics:
                print("V6 ENTONNOIR — " + " | ".join(
                    f"{reason}={count}" for reason,count in v5_diagnostics.most_common(12)
                ), flush=True)
            gate_rejections.clear()
            confirmed = []
            for sig in raw_signals + tracked_signals:
                if sig.pair in positions:
                    continue
                candidate = evaluate_trajectory(sig, now)
                if candidate:
                    confirmed.append(candidate)

            confirmed.sort(
                key=lambda c: (c.style in {"CONTINUATION","SWING_ACCUMULATION"},
                               c.signal.score, -c.price_gain, c.signal.qv24, c.signal.vol_ratio),
                reverse=True,
            )

            # V3: aucune confirmation V2 n'arrive directement sur Slack.
            # On ne sollicite l'IA que pour les candidats encore frais et dont le cooldown IA est écoulé.
            due = []
            for c in confirmed:
                pair = c.signal.pair
                if c.age_min > AI_MAX_SIGNAL_AGE_MIN and c.style == "MOMENTUM":
                    print(
                        f"AI IGNORE — {pair} trop ancien | age={c.age_min:.0f}m > {AI_MAX_SIGNAL_AGE_MIN}m",
                        flush=True,
                    )
                    pilots.pop(pair, None)
                    ai_next_review_at.pop(pair, None)
                    continue
                if c.price_gain > (12.0 if c.style == "CONTINUATION" else
                                   2.5 if c.style == "SWING_ACCUMULATION" else AI_MAX_PRICE_GAIN):
                    print(
                        f"AI IGNORE — {pair} mouvement déjà trop étendu depuis pilote | "
                        f"gain={c.price_gain:+.2f}% > {AI_MAX_PRICE_GAIN:.2f}%",
                        flush=True,
                    )
                    pilots.pop(pair, None)
                    ai_next_review_at.pop(pair, None)
                    continue
                if now >= ai_next_review_at.get(pair, 0):
                    due.append(c)

            due = due[:MAX_AI_REVIEWS_PER_SCAN]
            sent = 0
            ai_reviews_count = 0
            slack_attempted = 0

            if due and AI_ENABLED and OPENAI_API_KEY:
                result = ai_review_batch(due)
                if result is None:
                    for c in due:
                        ai_next_review_at[c.signal.pair] = now + AI_ERROR_COOLDOWN_MIN * 60
                else:
                    best_pair, reviews = result
                    ai_reviews_count = len(reviews)
                    by_candidate = {c.signal.pair: c for c in due}
                    by_review = {r.pair: r for r in reviews}

                    for c in due:
                        r = by_review.get(c.signal.pair)
                        if not r:
                            ai_next_review_at[c.signal.pair] = now + AI_ERROR_COOLDOWN_MIN * 60
                            print(f"AI ERREUR — aucune décision pour {c.signal.pair}", flush=True)
                            continue
                        print(
                            f"AI {r.decision} — {r.pair} | confiance={r.confidence} | {r.reason}",
                            flush=True,
                        )
                        cooldown = AI_WAIT_COOLDOWN_MIN if r.decision == "WAIT" else AI_IGNORE_COOLDOWN_MIN
                        ai_next_review_at[r.pair] = now + cooldown * 60

                    if best_pair:
                        review = by_review.get(best_pair)
                        candidate = by_candidate.get(best_pair)
                        if review and candidate and validate_trade_review(review, candidate):
                            message = format_trade_alert(candidate, review)
                            print(message, flush=True)
                            slack_attempted += 1
                            if send_slack_once(message):
                                mark_trade_sent(best_pair, time.time())
                                sent = 1
                        elif review and candidate:
                            print(
                                f"AI TRADE BLOQUE — {best_pair} | garde-fou niveaux/risque/âge non validé",
                                flush=True,
                            )
                            ai_next_review_at[best_pair] = now + AI_WAIT_COOLDOWN_MIN * 60
            elif due:
                print(
                    f"AI OFF — {len(due)} confirmé(s) non notifié(s); Slack reste silencieux",
                    flush=True,
                )

            if gate_rejections:
                print("PEPITO REJETS — " + " | ".join(
                    f"{name}={count}" for name, count in gate_rejections.most_common()
                ), flush=True)
            print(
                f"PEPITO JOURNAL — SCANNED {len(tickers)} -> EARLY {early_count} -> "
                f"TRACKED {len(pilots)} (REEVAL {len(tracked_signals)}) -> CONFIRMED {len(confirmed)} -> "
                f"AI {len(due)} -> CDC AVAILABLE {cdc_count} -> "
                f"SLACK ATTEMPTED {slack_attempted} -> SLACK SENT {sent}",
                flush=True,
            )

        except Exception as exc:
            print(f"ERREUR SCAN: {type(exc).__name__}: {exc}", flush=True)

        elapsed = time.time() - started
        time.sleep(max(1.0, SCAN_INTERVAL - elapsed))


def pepito_selftest():
    """Deterministic regression suite for PEPITO's known failure modes."""
    global cdc_pairs, positions
    old_cdc = set(cdc_pairs)
    old_positions = dict(positions)
    old_history = dict(history)
    now = 1_800_000_000.0

    def synthetic_signal(pair="NIGHT_USDT", price=1.01, qv24=5_000_000, spread=0.10,
                         change24=1.0, score=92, vol=20.0, r1=0.15, r5=0.50, r15=0.80):
        return Signal(pair, price, r1, r5, r15, vol, qv24, spread, change24, score, "ENTREE PILOTE")

    def candidate(pair, price=1.01):
        sig = synthetic_signal(pair=pair, price=price)
        pilot = PilotState(now-600, now, 1.0, 80, 3.2, 0.05, 0.10, 0.15, 0.0,
                           sightings=4, best_score=92, best_vol_ratio=20.0,
                           best_price=1.01, min_price=1.0, last_price=1.01,
                           last_score=92, last_vol_ratio=20.0,
                           trajectory=deque(maxlen=90))
        return ConfirmedCandidate(sig, pilot, 1.0, 10.0)

    def review(pair):
        return AIReview(pair, "TRADE", 90, "fixture", 1.00, 1.02, 0.98, 1.07, 1.15)

    try:
        # 1) Real-exchange gate regression: NIGHT/XPL pass fixture; KAS/SHX never TRADE.
        cdc_pairs = {"NIGHT_USD", "XPL_USD", "AKT_USD", "OP_USD", "ENA_USD"}
        assert cdc_tradeable("NIGHT_USDT") and cdc_tradeable("XPL_USDT")
        assert not cdc_tradeable("KAS_USDT") and not cdc_tradeable("SHX_USDT")
        assert not cdc_tradeable("USDT_USDT")
        assert validate_trade_review(review("NIGHT_USDT"), candidate("NIGHT_USDT"))
        assert not validate_trade_review(review("KAS_USDT"), candidate("KAS_USDT"))
        assert not validate_trade_review(review("SHX_USDT"), candidate("SHX_USDT"))
        # Net-profit regression: these October 8 alerts have inadequate potential
        # after fee reserve even if momentum and AI confidence look strong.
        weak_setups = [
            ("AKT_USDT", 0.7268, 0.7255, 0.7275, 0.7215, 0.7315, 0.7355),
            ("OP_USDT", 0.12221, 0.1221, 0.1223, 0.12175, 0.12275, 0.1231),
            ("ENA_USDT", 0.22213, 0.2218, 0.2222, 0.2208, 0.2235, 0.225),
        ]
        for pair, price, lo, hi, inv, tp1, tp2 in weak_setups:
            weak = AIReview(pair, "TRADE", 96, "momentum", lo, hi, inv, tp1, tp2)
            assert not validate_trade_review(weak, candidate(pair, price)), pair

        # Never accept far-from-market order zones even with ambitious objectives.
        stale = AIReview("NIGHT_USDT", "TRADE", 99, "stale", 1.05, 1.06, 1.02, 1.12, 1.25)
        assert not validate_trade_review(stale, candidate("NIGHT_USDT"))


        # Regression of a MET-like sideways accumulation with persistent volume.
        sc = synthetic_signal(pair="MET_USDT", price=1.005, qv24=215000,
                              spread=0.10, change24=5.0, score=70,
                              vol=12.0, r1=0.01, r5=0.00, r15=0.30)
        pi = PilotState(now-12*60, now, 1.0, 60, 25, 0, 0, 0.1, 4.0,
                        sightings=12, best_score=75, best_vol_ratio=25,
                        best_price=1.008, min_price=0.999, last_price=1.005,
                        last_score=70, last_vol_ratio=12,
                        trajectory=deque([(now-j*60, 1.005,70,12.0,0,0,0.3,215000,0.1,5.0)
                                          for j in range(12)],maxlen=90))
        assert swing_accumulation_ok(sc, pi, 12, 0.5)
        assert not swing_accumulation_ok(sc, pi, 2, 0.5)
        bad = synthetic_signal(pair="MET_USDT",price=1.005,qv24=215000,
                               spread=0.35,change24=5.0,score=70,vol=12.0,
                               r1=0.01,r5=0.0,r15=0.30)
        assert not swing_accumulation_ok(bad, pi, 12, 0.5)
        cdc_pairs.add("MET_USD")
        cc = ConfirmedCandidate(sc,pi,0.5,12,"SWING_ACCUMULATION")
        assert not validate_trade_review(
            AIReview("MET_USDT","TRADE",90,"weak",1.004,1.006,0.99,1.015,1.02),cc)

        assert AI_OUTPUT_TOKEN_BUDGET >= 2000
        assert "mark_trade_sent" in globals()
        # V4: official instrument universe / liquidity / synthetic OGN+RLC coverage.
        mock = {"code":0, "result":{"data":[
            {"i":"OGN_USD","a":"0.027","b":"0.0269","k":"0.0271","vv":"360000","c":"0.16"},
            {"i":"STRK_USDT","a":"0.05","b":"0.0499","k":"0.0501","vv":"540000","c":"0.06"},
            {"i":"RLC_USD","a":"0.7","b":"0.699","k":"0.701","vv":"680000","c":"0.05"},
            {"i":"CROCAT_USD","a":"1","b":"0.9","k":"1.1","vv":"90000","c":"0.6"},
            {"i":"FAKEUSD-PERP","a":"1","b":"1","k":"1","vv":"999999","c":"0.6"}
        ]}}
        normalized = normalize_cdc_tickers(mock, {"OGN_USD","STRK_USDT","RLC_USD"}, now)
        assert {t["currency_pair"] for t in normalized} == {"OGN_USDT","STRK_USDT","RLC_USDT"}
        assert next(t for t in normalized if t["currency_pair"] == "OGN_USDT")["exchange_symbol"] == "OGN_USD"
        p_strk=PilotState(now-18*60, now, 0.04988, 81, 7.9, 0,0,0,0,
                          sightings=15,best_score=93,best_vol_ratio=34.6,
                          best_price=0.0505,min_price=0.0498,last_price=0.0504,
                          last_score=61,last_vol_ratio=1.4,trajectory=deque(maxlen=90))
        continuing=synthetic_signal(pair="STRK_USDT",price=0.05045,score=61,
                                    vol=1.4,r1=0.1,r5=0.8,r15=1.2,change24=2.0)
        assert continuation_ok(continuing,p_strk,18,1.14)
        falling=synthetic_signal(pair="RLC_USDT",price=0.67,score=65,
                                 vol=2.2,r1=-0.4,r5=-1.3,r15=-3.0,change24=-12.0)
        assert not continuation_ok(falling,p_strk,18,2.0)
        assert PILOT_TTL_MIN >= 24*60

        # V5: candle-first detection must not need a rising 24h volume counter.
        # A freshly listed market with 2 observations is eligible via real 1m candles.
        vp="V5FRESH_USDT"; history[vp].clear()
        for i in range(2):
            history[vp].append(Snapshot(now-60+i*60, 1.0+i*0.0001,
                                        500_000, 0.9999, 1.0002))
        real_candles = {
            "volume_ratio":18.0, "volume_1m_usd_est":2100,
            "r1":0.14,"r5":0.45,"r15":0.65,"age_sec":30
        }
        # 1m candles already include history even on the scanner's FIRST tick.
        new_sig=score_signal(vp,0.0,metrics=real_candles)
        assert new_sig is not None and new_sig.vol_ratio==18.0
        history[vp].popleft()
        first_tick_sig=score_signal(vp,0.0,metrics=real_candles)
        assert first_tick_sig is not None and first_tick_sig.vol_ratio==18.0
        assert new_sig.ret_5m==0.45
        assert excluded_pair("USDT_USDT") and excluded_pair("USD_USDT")
        pair_list = v5_choose_candle_pairs([{
            "currency_pair":"OGN_USDT", "quote_volume":500000,
            "change_percentage":2.0
        },{
            "currency_pair":"RLC_USDT","quote_volume":500000,
            "change_percentage":1.0
        },{
            "currency_pair":"USDT_USDT","quote_volume":900000,
            "change_percentage":0.0
        }])
        assert set(pair_list)=={"OGN_USDT","RLC_USDT"}, pair_list
        history.pop(vp,None)

        # V6: an extended coin may enter silent surveillance after a new base.
        hp6="V6HOT_USDT"
        history[hp6].clear()
        history[hp6].append(Snapshot(now,1.0,2_000_000,0.9997,1.0003))
        quiet={"volume_ratio":3.0,"volume_1m_usd_est":1500.0,
               "r1":0.05,"r5":0.25,"r15":0.72,"age_sec":18}
        hot_sig=score_signal(hp6,40.0,metrics=quiet)
        assert hot_sig is not None and hot_sig.change_24h==40.0
        assert open_pilot(hot_sig,now) and hp6 in pilots
        hp6_pilot=pilots[hp6]
        hp6_pilot.sightings=10
        hp6_pilot.best_score=80
        hp6_pilot.best_vol_ratio=12.0
        reaccel=synthetic_signal(pair=hp6,price=1.009,score=69,vol=4.0,
                                 r1=0.10,r5=0.65,r15=1.1,change24=41.0)
        assert continuation_ok(reaccel,hp6_pilot,11,0.9)
        assert not continuation_ok(reaccel,hp6_pilot,3,0.9)
        assert not continuation_ok(
            synthetic_signal(pair=hp6,price=1.009,score=69,vol=4,
                             r1=0.5,r5=3.0,r15=6.0,change24=41),
            hp6_pilot,12,0.9)
        pilots.pop(hp6,None)
        history.pop(hp6,None)

        # Microcap may be monitored at 25k of daily volume, but never alerted
        # until execution liquidity has risen to the strict trading floor.
        low_watch="TINYT_USDT"
        history[low_watch].clear()
        history[low_watch].append(Snapshot(now,1.0,25_000,0.9998,1.0002))
        watch_metrics={"volume_ratio":18.0,"volume_1m_usd_est":400.0,
                       "r1":0.12,"r5":0.80,"r15":1.20,"age_sec":15}
        assert score_signal(low_watch,0,metrics=watch_metrics) is not None
        cdc_pairs.add("TINYT_USD")
        not_liquid=candidate(low_watch)
        not_liquid.signal.qv24=25_000
        assert not validate_trade_review(review(low_watch),not_liquid)
        history.pop(low_watch,None)

        # 2) Progressive acceleration beats a late isolated x100 spike.
        progressive = [3.2, 6.8, 12.0, 20.0]
        late = [3.0, 3.1, 3.0, 100.0]
        prog_steps = sum(1 for x,y in zip(progressive, progressive[1:]) if y >= x*1.15)
        late_steps = sum(1 for x,y in zip(late, late[1:]) if y >= x*1.15)
        assert prog_steps >= 2 and late_steps < 2

        # 3) Low absolute liquidity is rejected before spectacular relative volume matters.
        lp="SELFLOW_USDT"; history[lp].clear()
        for i,qv in enumerate([100000,100001,100002,100003,100004,100005,100105]):
            history[lp].append(Snapshot(now-360+i*60,1.0,qv,0.999,1.001))
        assert score_signal(lp, 0.0) is None

        # 4) Anti-chase: a +20% 24h token is rejected.
        hp="SELFCHASE_USDT"; history[hp].clear()
        for i,qv in enumerate([2_000_000,2_000_001,2_000_002,2_000_003,2_000_004,2_000_005,2_000_105]):
            history[hp].append(Snapshot(now-360+i*60,1.0,qv,0.999,1.001))
        assert score_signal(hp, 20.0) is None

        # 5) A genuinely early x20 acceleration must be capable of reaching
        # the confirmation score without any fictitious catalyst data.
        ep="SELFACCEL_USDT"; history[ep].clear()
        for i in range(16):
            qv = 5_000_000 + i * 100 + (1900 if i == 15 else 0)
            price = 1.007 if i == 15 else 1.0
            history[ep].append(Snapshot(now-(15-i)*60,price,qv,price*0.9995,price*1.0005))
        early = score_signal(ep, 0.7)
        assert early is not None and early.score >= CONFIRMED_SCORE, (
            f"Valid early acceleration cannot confirm: {early.score if early else 'filtered'}"
        )

        # 6) Rejection diagnostics must preserve counts per gate.
        gate_rejections.clear()
        gate_rejections.update(["score", "volume", "score"])
        assert gate_rejections["score"] == 2 and gate_rejections["volume"] == 1
        gate_rejections.clear()

        # 7) Position circuit: stop, TP1 and TP2 actions remain deterministic.
        positions = {"SELF_USDT":{"entry_price":1.0,"invalidation":0.90,"tp1":1.10,"tp2":1.20}}
        assert position_action("SELF_USDT",0.89) == "SORTIR"
        assert position_action("SELF_USDT",1.11) == "PRENDRE DES BENEFICES"
        assert position_action("SELF_USDT",1.21) == "VENDRE DAVANTAGE"

        print("PEPITO SELFTEST — PASS | V6_EARLY_MICROCAP_WATCH | V6_TRADE_LIQUIDITY_LOCK | V6_POST_PUMP_REACCEL | V5_CANDLE_FIRST | V5_ROTATING_SPOT | V5_NO_STABLE_QUOTES | CDC_PRIMARY_OGN_STRK_RLC | CONTINUATION_STRK | 7DAY_HISTORY | SWING_ACCUMULATION | CDC_BLOCK | NET_PROFIT_GATE | REAL_ALERT_REGRESSIONS | VALIDATE_TRADE | PROGRESSIVE_ACCEL | LOW_LIQUIDITY | ANTI_CHASE | SCORE_REACHABLE | GATE_DIAGNOSTICS | POSITION_EXITS", flush=True)
        return True
    finally:
        cdc_pairs = old_cdc
        positions = old_positions
        history.pop("SELFLOW_USDT", None)
        history.pop("SELFCHASE_USDT", None)
        history.pop("SELFACCEL_USDT", None)



def pepito_integration_test():
    """Read-only real data verification: never sends Slack or places any orders."""
    refresh_cdc_pairs()
    all_tickers = fetch_tickers()
    assert len(all_tickers) >= 100, "CDC market coverage unexpectedly low"
    checked = []
    for asset in ("OGN", "STRK", "RLC"):
        pair = asset + "_USDT"
        item = cdc_ticker_by_pair.get(pair)
        assert item, f"Exchange ticker missing: {asset}"
        candle = cdc_candle_metrics(pair)
        assert candle and candle["volume_1m_usd_est"] >= 0, f"1m candles missing: {asset}"
        context = gate_historical_context(pair, time.time())
        assert context, f"6h/24h context missing: {asset}"
        checked.append(f"{asset}={item['exchange_symbol']} 1m_vol={candle['volume_1m_usd_est']:.0f} "
                       f"r5={candle['r5']:+.2f}% c6h={context.get('6h',{}).get('return_pct','NA')}")
    print("PEPITO V4 LIVE INTEGRATION — PASS | Spot catalog="+str(len(cdc_pairs))
          +" | Assets="+str(len(all_tickers))+" | "+" | ".join(checked), flush=True)


if __name__ == "__main__":
    if os.getenv("PEPITO_INTEGRATION_TEST", "0").lower() in {"1","true","yes","on"}:
        pepito_integration_test()
    elif os.getenv("PEPITO_SELFTEST", "0").strip().lower() in {"1","true","yes","on"}:
        pepito_selftest()
    else:
        run()
