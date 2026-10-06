#!/usr/bin/env python3
import os
import time
import statistics
import json
import sqlite3
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional

import requests

GATE_TICKERS_URL = "https://api.gateio.ws/api/v4/spot/tickers"
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
    _state_db.execute("DELETE FROM market_history WHERE bucket_ts < ?", (int(time.time()) - 8*24*3600,))
    _state_db.commit()
    print(f"PEPITO STATE — SQLite actif | {STATE_DB_PATH}", flush=True)

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

def persistent_context(pair, now):
    if _state_db is None:
        return {}
    rows = _state_db.execute("SELECT bucket_ts,price,qv24 FROM market_history WHERE pair=? AND bucket_ts>=? ORDER BY bucket_ts",
        (pair,int(now)-7*24*3600)).fetchall()
    if not rows:
        return {}
    out = {}
    for label,sec in (("6h",21600),("24h",86400),("7d",604800)):
        eligible=[r for r in rows if r[0] <= now-sec]
        if eligible:
            r=eligible[-1]
            out[label]={"price":r[1],"return_pct":pct(rows[-1][1],r[1]),"qv24":r[2]}
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
    r = session.get(CDC_INSTRUMENTS_URL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    payload = r.json()
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
    if pair in positions:
        return None
    if excluded_pair(pair):
        return

    price = fnum(ticker.get("last"))
    qv24 = fnum(ticker.get("quote_volume"))
    bid = fnum(ticker.get("highest_bid"))
    ask = fnum(ticker.get("lowest_ask"))
    if price <= 0 or qv24 <= 0:
        return

    snap = Snapshot(now, price, qv24, bid, ask)\n    history[pair].append(snap)\n    persist_market_snapshot(pair, snap)


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


def score_signal(pair: str) -> Optional[Signal]:
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
    change24h = pct(cur.price, s24.price) if s24 else 0.0

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
    if vol_ratio < EARLY_MIN_VOLUME_RATIO:
        return None
    if ret1 < -0.35 or ret5 < -0.75:
        return None
    if ret5 > EARLY_MAX_RET5 or ret15 > 10.0:
        return None

    # PEPITO scoring verrouillé: 30/20/15/15/10/10.
    # Les données absentes ne sont jamais inventées: catalyst=0 tant qu'un flux fiable
    # n'est pas branché; flow utilise ici un proxy momentum conservateur.
    volume_pts = min(30, max(0, int((vol_ratio - 3.0) * 1.6)))
    flow_pts = min(20, max(0, int(max(ret1, 0) * 8 + max(ret5, 0) * 3)))
    liquidity_pts = min(15, max(0, int(10 * (1 - spread / MAX_SPREAD_PCT) + min(5, max(0, math.log10(max(cur.qv24, 1)) - 5)))))
    extension = max(abs(ret5), max(change24h, 0))
    unextended_pts = 15 if extension <= 2 else (10 if extension <= 5 else (4 if extension <= 10 else 0))
    structure_pts = min(10, max(0, int((max(ret5, 0) + max(ret15, 0) * 0.5) * 4)))
    catalyst_pts = 0
    score = volume_pts + flow_pts + liquidity_pts + unextended_pts + structure_pts + catalyst_pts

    # Anti-chase: un x50/x100 déjà très étendu ne devient pas prioritaire.
    if change24h > 15.0 or ret15 > 6.0:
        return None

    # PEPITO: détection précoce silencieuse. Un score inférieur au seuil pilote
    # peut être mémorisé si le volume accélère déjà; aucune notification Slack ici.
    early_score_floor = max(35, PILOT_SCORE - 35)
    if score < early_score_floor:
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

    print(
        f"PILOTE SUIVI — {sig.pair} | age={age_min:.0f}m | vues={pilot.sightings} | "
        f"score={sig.score} | gain={price_gain:+.2f}% | vol=x{sig.vol_ratio:.1f} | "
        f"r5={sig.ret_5m:+.2f}% | r15={sig.ret_15m:+.2f}% | "
        f"24h={sig.change_24h:+.2f}%",
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
            body = (r.text or "")[:300].replace("\n", " ")
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
    # La zone proposée doit rester proche du marché actuel; sinon l'alerte est déjà périmée.
    if abs(pct(c.signal.price, (lo + hi) / 2.0)) > 3.0:
        return False
    risk = ((lo + hi) / 2.0) - inv
    reward1 = tp1 - ((lo + hi) / 2.0)
    if risk <= 0 or reward1 / risk < 1.0:
        return False
    if c.age_min > AI_MAX_SIGNAL_AGE_MIN or c.price_gain > AI_MAX_PRICE_GAIN:
        return False
    return True


def format_trade_alert(c: ConfirmedCandidate, review: AIReview) -> str:
    s = c.signal
    return (
        f"🚨🟢 CRYPTO RADAR — TRADE\n"
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
        f"Source marché : Gate.io | Filtre V2 + arbitrage IA V3"
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

    init_state_db()\n\n    while True:
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

            commit_state(now)\n            cleanup_expired_pilots(now)

            raw_signals = []
            for pair in list(history.keys()):
                sig = score_signal(pair)
                if not sig:
                    continue
                sig.change_24h = fnum(by_pair.get(pair, {}).get("change_percentage"))
                raw_signals.append(sig)

            raw_signals.sort(key=lambda x: (x.score, x.vol_ratio), reverse=True)
            early_count = len(raw_signals)
            cdc_count = sum(1 for s in raw_signals if cdc_tradeable(s.pair))

            confirmed = []
            for sig in raw_signals:
                candidate = evaluate_trajectory(sig, now)
                if candidate:
                    confirmed.append(candidate)

            confirmed.sort(
                key=lambda c: (c.signal.score, c.price_gain, c.signal.vol_ratio),
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
                            print("\n" + message, flush=True)
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

            print(
                f"PEPITO JOURNAL — SCANNED {len(tickers)} -> EARLY {early_count} -> "
                f"TRACKED {len(pilots)} -> CONFIRMED {len(confirmed)} -> "
                f"AI {len(due)} -> CDC AVAILABLE {cdc_count} -> "
                f"SLACK ATTEMPTED {slack_attempted} -> SLACK SENT {sent}",
                flush=True,
            )

        except Exception as exc:
            print(f"ERREUR SCAN: {type(exc).__name__}: {exc}", flush=True)

        elapsed = time.time() - started
        time.sleep(max(1.0, SCAN_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
