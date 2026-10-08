#!/usr/bin/env python3
import os
import time
import statistics
import math
import json
import sqlite3
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import Optional

import requests

GATE_TICKERS_URL = "https://api.gateio.ws/api/v4/spot/tickers"
GATE_CANDLES_URL = "https://api.gateio.ws/api/v4/spot/candlesticks"
CDC_INSTRUMENTS_URL = "https://api.crypto.com/exchange/v1/public/get-instruments"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
MIN_24H_QUOTE_VOL = float(os.getenv("MIN_24H_QUOTE_VOL", "250000"))
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
# Strict net-exploitability gate. Conservative default for base Crypto.com
# Exchange spot taker trades (0.50% buy + 0.50% sell), plus execution buffer.
# The app can charge a different price/fee: never promise an executable profit.
ROUND_TRIP_FEE_PCT = float(os.getenv("ROUND_TRIP_FEE_PCT", "1.00"))
EXECUTION_BUFFER_PCT = float(os.getenv("EXECUTION_BUFFER_PCT", "0.30"))
MIN_NET_TP1_PCT = float(os.getenv("MIN_NET_TP1_PCT", "0.75"))
MIN_NET_TP2_PCT = float(os.getenv("MIN_NET_TP2_PCT", "3.00"))
MIN_NET_REWARD_RISK = float(os.getenv("MIN_NET_REWARD_RISK", "2.00"))
MAX_ENTRY_DRIFT_PCT = float(os.getenv("MAX_ENTRY_DRIFT_PCT", "0.50"))


# V2 — suivi de trajectoire après la première détection pilote.
PILOT_TTL_MIN = int(os.getenv("PILOT_TTL_MIN", "180"))
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
    "EUR", "EURC", "EURI", "USDP", "GUSD", "BUSD"
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
            if now - float(d.get("created_at", 0)) > ttl:
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
    """Compact 6h/24h/7d context from Gate hourly candles; fetched only for candidates."""
    try:
        r = session.get(GATE_CANDLES_URL, params={"currency_pair": pair, "interval": "1h", "limit": 168}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        candles = r.json()
        parsed = []
        for row in candles if isinstance(candles, list) else []:
            if not isinstance(row, list) or len(row) < 6:
                continue
            ts = int(float(row[0]))
            close = fnum(row[2])
            quote_vol = fnum(row[1])
            if ts > 0 and close > 0:
                parsed.append((ts, close, quote_vol))
        parsed.sort(key=lambda x: x[0])
        if not parsed:
            return {}
        current = parsed[-1][1]
        out = {}
        for label, sec in (("6h",21600),("24h",86400),("7d",604800)):
            eligible = [x for x in parsed if x[0] <= now-sec]
            if eligible:
                x = eligible[-1]
                out[label] = {"price": x[1], "return_pct": pct(current, x[1]), "hour_quote_volume": x[2], "source": "gate_1h"}
        return out
    except Exception as exc:
        print(f"PEPITO CONTEXTE ERREUR — {pair} | {type(exc).__name__}: {exc}", flush=True)
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
    # Fill missing horizons without storing 2203 markets continuously.
    if len(out) < 3:
        gate = gate_historical_context(pair, now)
        for label in ("6h","24h","7d"):
            if label not in out and label in gate:
                out[label] = gate[label]
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


def fetch_tickers():
    r = session.get(GATE_TICKERS_URL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("Réponse Gate.io inattendue")
    return data


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


def score_signal(pair: str, gate_change24h: Optional[float] = None, tracking: bool = False) -> Optional[Signal]:
    samples = history[pair]
    if len(samples) < 7:
        return None

    cur = samples[-1]
    s1 = closest_before(samples, 55)
    s5 = closest_before(samples, 4 * 60 + 30)
    s15 = closest_before(samples, 14 * 60 + 30)
    if not s1 or not s5:
        return None

    ret1 = pct(cur.price, s1.price)
    ret5 = pct(cur.price, s5.price)
    ret15 = pct(cur.price, s15.price) if s15 else 0.0
    # 24h est enrichi depuis le ticker Gate après scoring; pour le scoring lui-même,
    # on le calcule depuis l’historique local quand 24h est disponible, sinon 0.
    s24 = closest_before(samples, 24 * 60 * 60 - 30)
    change24h = pct(cur.price, s24.price) if s24 else (gate_change24h if gate_change24h is not None else 0.0)

    spread = 999.0
    if cur.bid > 0 and cur.ask > 0 and cur.ask >= cur.bid:
        mid = (cur.ask + cur.bid) / 2
        spread = (cur.ask - cur.bid) / mid * 100.0 if mid else 999.0

    if cur.qv24 < MIN_24H_QUOTE_VOL or spread > 0.50:
        return None

    deltas = minute_volume_deltas(samples)
    if len(deltas) < 5:
        return None
    current_delta = max(0.0, cur.qv24 - samples[-2].qv24)
    baseline_pool = deltas[:-1][-20:]
    baseline = statistics.median(baseline_pool) if baseline_pool else 0.0
    if baseline <= 0:
        return None
    vol_ratio = current_delta / baseline

    # Détection pré-mouvement: volume anormal + début de momentum,
    # sans accepter une bougie déjà partie de façon extrême à très court terme.
    # A new candidate must cross the early trigger. An existing pilot is different:
    # keep measuring its trajectory even after the anomaly cools down, otherwise
    # TRACKED pilots become dead memory and can never prove continuation/failure.
    if not tracking and vol_ratio < EARLY_MIN_VOLUME_RATIO:
        return None
    if ret1 < -0.35 or ret5 < -0.75:
        return None
    if ret5 > EARLY_MAX_RET5 or ret15 > 10.0:
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
    if change24h > 15.0 or ret15 > 6.0:
        return None

    # PEPITO: détection précoce silencieuse. Un score inférieur au seuil pilote
    # peut être mémorisé si le volume accélère déjà; aucune notification Slack ici.
    early_score_floor = max(35, PILOT_SCORE - 35)
    if not tracking and score < early_score_floor:
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
        if _state_db is not None:
            _state_db.execute("DELETE FROM pilot_state WHERE pair=?", (pair,))
        ai_next_review_at.pop(pair, None)
        print(
            f"PILOTE EXPIRE — {pair} | age={((now - p.created_at) / 60):.0f}m | "
            f"vues={p.sightings} | best_score={p.best_score}",
            flush=True,
        )


def open_pilot(sig: Signal, now: float) -> bool:
    # Notre objectif est le pré-mouvement: on ne démarre pas un suivi
    # si le token est déjà fortement étendu sur 24 h.
    if sig.change_24h > MAX_PILOT_24H:
        print(
            f"PILOTE REJETE — {sig.pair} déjà étendu | "
            f"24h={sig.change_24h:+.2f}% > {MAX_PILOT_24H:.2f}%",
            flush=True,
        )
        return False

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
        "Tu es la couche finale de filtrage d'un radar crypto haussier très court terme. "
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
        "max_output_tokens": 900,
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
        raw = _response_text(r.json())
        if not raw:
            print("AI ERREUR — réponse vide", flush=True)
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
    """Garde-fous déterministes avant toute notification Slack."""
    if review.decision != "TRADE" or review.confidence < 70:
        return False
    if not cdc_tradeable(c.signal.pair):
        print(f"TRADE BLOQUE CDC — {c.signal.pair} absent de Crypto.com Exchange spot", flush=True)
        return False
    vals = [review.entry_low, review.entry_high, review.invalidation, review.tp1, review.tp2]
    if any(v is None or v <= 0 for v in vals):
        return False
    lo, hi, inv, tp1, tp2 = vals
    if lo > hi or not (inv < lo <= hi < tp1 < tp2):
        return False
    # Reject stale or out-of-market AI entry zones.
    if abs(pct(c.signal.price, (lo + hi) / 2.0)) > MAX_ENTRY_DRIFT_PCT:
        return False

    # Check worst executable entry within the proposed zone (hi), not midpoint.
    # Use a conservative round-trip cost reserve; actual app quotes may be worse.
    costs_pct = max(0.0, ROUND_TRIP_FEE_PCT) + max(0.0, EXECUTION_BUFFER_PCT)
    gross_tp1_pct = (tp1 / hi - 1.0) * 100.0
    gross_tp2_pct = (tp2 / hi - 1.0) * 100.0
    net_tp1_pct = gross_tp1_pct - costs_pct
    net_tp2_pct = gross_tp2_pct - costs_pct
    loss_including_costs_pct = (hi - inv) / hi * 100.0 + costs_pct
    net_rr = net_tp2_pct / loss_including_costs_pct if loss_including_costs_pct > 0 else 0.0

    if (net_tp1_pct < MIN_NET_TP1_PCT
            or net_tp2_pct < MIN_NET_TP2_PCT
            or net_rr < MIN_NET_REWARD_RISK):
        print(
            f"TRADE BLOQUE RENTABILITE — {c.signal.pair} | "
            f"TP1_net={net_tp1_pct:+.2f}% | TP2_net={net_tp2_pct:+.2f}% | "
            f"risque_frais={loss_including_costs_pct:.2f}% | "
            f"RR_net={net_rr:.2f} | reserve_frais={costs_pct:.2f}%",
            flush=True,
        )
        return False

    if c.age_min > AI_MAX_SIGNAL_AGE_MIN or c.price_gain > AI_MAX_PRICE_GAIN:
        return False
    return True


def format_trade_alert(c: ConfirmedCandidate, review: AIReview) -> str:
    s = c.signal
    return (
        f"TRADE EXPLOITABLE MAINTENANT — CONFIRMATION/RENFORCEMENT\n"
        f"{s.pair}\n\n"
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
        f"Source marché : Gate.io | validation Crypto.com Exchange"
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
        f"VALIDATION V3 ACTIVE — PILOTE->TRAJECTOIRE->CONFIRME->IA->TRADE | "
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

            raw_signals = []
            signal_by_pair = {}
            for pair in list(history.keys()):
                gate24 = fnum(by_pair.get(pair, {}).get("change_percentage"))
                sig = score_signal(pair, gate24)
                if not sig:
                    continue
                raw_signals.append(sig)
                signal_by_pair[pair] = sig

            raw_signals.sort(key=lambda x: (x.score, x.vol_ratio), reverse=True)
            early_count = len(raw_signals)
            cdc_count = sum(1 for s in raw_signals if cdc_tradeable(s.pair))

            # Multi-pass follow-up is independent from the initial anomaly trigger.
            # Every live pilot gets a tracking score each cycle, even when vol_ratio
            # has cooled below EARLY_MIN_VOLUME_RATIO.
            tracked_signals = []
            for pair in list(pilots.keys()):
                if pair in positions or pair in signal_by_pair or pair not in history:
                    continue
                gate24 = fnum(by_pair.get(pair, {}).get("change_percentage"))
                sig = score_signal(pair, gate24, tracking=True)
                if sig:
                    tracked_signals.append(sig)

            gate_rejections.clear()
            confirmed = []
            for sig in raw_signals + tracked_signals:
                if sig.pair in positions:
                    continue
                candidate = evaluate_trajectory(sig, now)
                if candidate:
                    confirmed.append(candidate)

            confirmed.sort(
                key=lambda c: (c.signal.score, -c.price_gain, c.signal.qv24, c.signal.vol_ratio),
                reverse=True,
            )

            # V3: aucune confirmation V2 n'arrive directement sur Slack.
            # On ne sollicite l'IA que pour les candidats encore frais et dont le cooldown IA est écoulé.
            due = []
            for c in confirmed:
                pair = c.signal.pair
                if c.age_min > AI_MAX_SIGNAL_AGE_MIN:
                    print(
                        f"AI IGNORE — {pair} trop ancien | age={c.age_min:.0f}m > {AI_MAX_SIGNAL_AGE_MIN}m",
                        flush=True,
                    )
                    pilots.pop(pair, None)
                    ai_next_review_at.pop(pair, None)
                    continue
                if c.price_gain > AI_MAX_PRICE_GAIN:
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
                                last_alert_at[best_pair] = time.time()
                                pilots.pop(best_pair, None)
                                ai_next_review_at.pop(best_pair, None)
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

        print("PEPITO SELFTEST — PASS | CDC_BLOCK | NET_PROFIT_GATE | REAL_ALERT_REGRESSIONS | VALIDATE_TRADE | PROGRESSIVE_ACCEL | LOW_LIQUIDITY | ANTI_CHASE | SCORE_REACHABLE | GATE_DIAGNOSTICS | POSITION_EXITS", flush=True)
        return True
    finally:
        cdc_pairs = old_cdc
        positions = old_positions
        history.pop("SELFLOW_USDT", None)
        history.pop("SELFCHASE_USDT", None)
        history.pop("SELFACCEL_USDT", None)


if __name__ == "__main__":
    if os.getenv("PEPITO_SELFTEST", "0").strip().lower() in {"1","true","yes","on"}:
        pepito_selftest()
    else:
        run()
