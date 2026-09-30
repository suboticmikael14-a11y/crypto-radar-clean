#!/usr/bin/env python3
import os
import time
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional

import requests

GATE_TICKERS_URL = "https://api.gateio.ws/api/v4/spot/tickers"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
MIN_24H_QUOTE_VOL = float(os.getenv("MIN_24H_QUOTE_VOL", "250000"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "6"))
PILOT_SCORE = int(os.getenv("PILOT_SCORE", "78"))
CONFIRMED_SCORE = int(os.getenv("CONFIRMED_SCORE", "90"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "3"))
PAIR_COOLDOWN_MIN = int(os.getenv("PAIR_COOLDOWN_MIN", "60"))
SLACK_ENABLED = os.getenv("SLACK_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

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
HISTORY_MAX_MIN = 35

EXCLUDED_BASES = {
    "USDC", "USDE", "USDS", "FDUSD", "TUSD", "DAI", "PYUSD", "USD1",
    "EUR", "EURC", "EURI", "USDP", "GUSD", "BUSD"
}

session = requests.Session()
session.headers.update({"Accept": "application/json", "User-Agent": "crypto-radar-clean/2.0"})

history = defaultdict(lambda: deque(maxlen=HISTORY_MAX_MIN + 10))
last_alert_at = {}
pilots = {}
slack_blocked_until = 0.0
last_slack_send_at = 0.0


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


@dataclass
class ConfirmedCandidate:
    signal: Signal
    pilot: PilotState
    price_gain: float
    age_min: float


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

    history[pair].append(Snapshot(now, price, qv24, bid, ask))


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
    if vol_ratio < MIN_VOLUME_RATIO:
        return None
    if ret1 < -0.35 or ret5 < -0.75:
        return None
    if ret5 > 5.0 or ret15 > 10.0:
        return None

    score = 0

    # Volume relatif: 35 pts max
    if vol_ratio >= 20:
        score += 35
    elif vol_ratio >= 12:
        score += 30
    elif vol_ratio >= 8:
        score += 24
    elif vol_ratio >= 6:
        score += 18

    # Momentum 5 min: hausse visible mais encore modérée.
    if 0.35 <= ret5 <= 2.5:
        score += 25
    elif 0.10 <= ret5 < 0.35:
        score += 20
    elif -0.25 <= ret5 < 0.10:
        score += 12
    elif 2.5 < ret5 <= 5.0:
        score += 10

    # Momentum 1 min
    if 0.05 <= ret1 <= 1.2:
        score += 15
    elif -0.10 <= ret1 < 0.05:
        score += 8
    elif 1.2 < ret1 <= 2.0:
        score += 6

    # Spread
    if spread <= 0.08:
        score += 15
    elif spread <= 0.15:
        score += 12
    elif spread <= 0.30:
        score += 7

    # Liquidité
    if cur.qv24 >= 5_000_000:
        score += 10
    elif cur.qv24 >= 1_000_000:
        score += 7
    elif cur.qv24 >= 500_000:
        score += 4

    if score < PILOT_SCORE:
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
        change_24h=0.0,
        score=score,
        level="ENTREE PILOTE",
    )


def can_alert(pair: str, now: float) -> bool:
    last = last_alert_at.get(pair, 0)
    return (now - last) >= PAIR_COOLDOWN_MIN * 60


def cleanup_expired_pilots(now: float):
    ttl = PILOT_TTL_MIN * 60
    expired = [pair for pair, p in pilots.items() if now - p.created_at > ttl]
    for pair in expired:
        p = pilots.pop(pair)
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
    )
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
        f"VALIDATION V2 ACTIVE — PILOTE->TRAJECTOIRE->CONFIRME | "
        f"pilot={PILOT_SCORE} | confirm={CONFIRMED_SCORE} | ttl={PILOT_TTL_MIN}m | "
        f"gain>={CONFIRM_MIN_PRICE_GAIN:.2f}% | r5>={CONFIRM_MIN_RET5:.2f}% | "
        f"r15>={CONFIRM_MIN_RET15:.2f}% | 24h<={MAX_PILOT_24H:.2f}%",
        flush=True,
    )

    while True:
        started = time.time()
        try:
            tickers = fetch_tickers()
            now = time.time()
            by_pair = {}

            for t in tickers:
                pair = t.get("currency_pair", "")
                by_pair[pair] = t
                add_snapshot(t, now)

            cleanup_expired_pilots(now)

            raw_signals = []
            for pair in list(history.keys()):
                sig = score_signal(pair)
                if not sig:
                    continue
                sig.change_24h = fnum(by_pair.get(pair, {}).get("change_percentage"))
                raw_signals.append(sig)

            # On traite d'abord les meilleurs signaux du scan.
            raw_signals.sort(key=lambda x: (x.score, x.vol_ratio), reverse=True)

            confirmed = []
            for sig in raw_signals:
                candidate = evaluate_trajectory(sig, now)
                if candidate:
                    confirmed.append(candidate)

            confirmed.sort(
                key=lambda c: (
                    c.signal.score,
                    c.price_gain,
                    c.signal.vol_ratio,
                ),
                reverse=True,
            )
            selected = confirmed[:MAX_ALERTS_PER_SCAN]

            sent = 0
            for candidate in selected:
                message = format_confirmed_alert(candidate)
                print("\n" + message, flush=True)
                if send_slack_once(message):
                    last_alert_at[candidate.signal.pair] = time.time()
                    pilots.pop(candidate.signal.pair, None)
                    sent += 1

            print(
                f"Scan terminé | {len(tickers)} tickers | "
                f"{len(raw_signals)} pilote(s) actif(s) ce scan | "
                f"{len(confirmed)} confirmation(s) | {sent} Slack | "
                f"{len(pilots)} pilote(s) suivi(s)",
                flush=True,
            )

        except Exception as exc:
            print(f"ERREUR SCAN: {type(exc).__name__}: {exc}", flush=True)

        elapsed = time.time() - started
        time.sleep(max(1.0, SCAN_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
