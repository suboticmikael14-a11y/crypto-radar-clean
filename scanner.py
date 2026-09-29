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

HTTP_TIMEOUT = 15
HISTORY_MAX_MIN = 35

EXCLUDED_BASES = {
    "USDC", "USDE", "USDS", "FDUSD", "TUSD", "DAI", "PYUSD", "USD1",
    "EUR", "EURC", "EURI", "USDP", "GUSD", "BUSD"
}

session = requests.Session()
session.headers.update({"Accept": "application/json", "User-Agent": "crypto-radar-clean/1.0"})

history = defaultdict(lambda: deque(maxlen=HISTORY_MAX_MIN + 10))
last_alert_at = {}
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

    # Pré-mouvement haussier: volume anormal, début de momentum,
    # mais pas une bougie déjà partie de +8/+15%.
    if vol_ratio < MIN_VOLUME_RATIO:
        return None
    if ret1 < -0.35 or ret5 < -0.75:
        return None
    if ret5 > 5.0 or ret15 > 10.0:
        return None

    score = 0

    # Volume relatif: 35 pts max
    if vol_ratio >= 20: score += 35
    elif vol_ratio >= 12: score += 30
    elif vol_ratio >= 8: score += 24
    elif vol_ratio >= 6: score += 18

    # Momentum 5 min: le meilleur signal est une hausse déjà visible mais encore modérée.
    if 0.35 <= ret5 <= 2.5: score += 25
    elif 0.10 <= ret5 < 0.35: score += 20
    elif -0.25 <= ret5 < 0.10: score += 12
    elif 2.5 < ret5 <= 5.0: score += 10

    # Momentum 1 min
    if 0.05 <= ret1 <= 1.2: score += 15
    elif -0.10 <= ret1 < 0.05: score += 8
    elif 1.2 < ret1 <= 2.0: score += 6

    # Spread
    if spread <= 0.08: score += 15
    elif spread <= 0.15: score += 12
    elif spread <= 0.30: score += 7

    # Liquidité
    if cur.qv24 >= 5_000_000: score += 10
    elif cur.qv24 >= 1_000_000: score += 7
    elif cur.qv24 >= 500_000: score += 4

    level = "CONFIRME" if score >= CONFIRMED_SCORE else "ENTREE PILOTE"
    if score < PILOT_SCORE:
        return None

    # Gate fournit change_percentage sur le ticker; injecté plus bas.
    return Signal(
        pair=pair, price=cur.price, ret_1m=ret1, ret_5m=ret5, ret_15m=ret15,
        vol_ratio=vol_ratio, qv24=cur.qv24, spread_pct=spread,
        change_24h=0.0, score=score, level=level
    )


def can_alert(pair: str, now: float) -> bool:
    last = last_alert_at.get(pair, 0)
    return (now - last) >= PAIR_COOLDOWN_MIN * 60


def format_alert(s: Signal) -> str:
    icon = "🟢" if s.level == "ENTREE PILOTE" else "✅"
    return (
        f"{icon} CRYPTO RADAR CLEAN — {s.level}\n"
        f"{s.pair}\n\n"
        f"Score : {s.score}/100\n"
        f"Prix : {s.price:.10g}\n"
        f"Variation ~1 min : {s.ret_1m:+.2f}%\n"
        f"Variation ~5 min : {s.ret_5m:+.2f}%\n"
        f"Variation ~15 min : {s.ret_15m:+.2f}%\n"
        f"Volume relatif : x{s.vol_ratio:.1f}\n"
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

            signals = []
            for pair in list(history.keys()):
                sig = score_signal(pair)
                if sig and can_alert(pair, now):
                    sig.change_24h = fnum(by_pair.get(pair, {}).get("change_percentage"))
                    signals.append(sig)

            signals.sort(key=lambda x: (x.level == "CONFIRME", x.score, x.vol_ratio), reverse=True)
            selected = signals[:MAX_ALERTS_PER_SCAN]

            sent = 0
            for sig in selected:
                message = format_alert(sig)
                print("\n" + message, flush=True)
                if send_slack_once(message):
                    last_alert_at[sig.pair] = time.time()
                    sent += 1
                elif not SLACK_ENABLED or not SLACK_WEBHOOK_URL:
                    # En mode logs seuls, ne pas marquer le cooldown:
                    # cela permet de voir les candidats à chaque scan de validation.
                    pass

            print(
                f"Scan terminé | {len(tickers)} tickers | "
                f"{len(signals)} signal(aux) | {len(selected)} sélectionné(s) | "
                f"{sent} Slack",
                flush=True,
            )

        except Exception as exc:
            print(f"ERREUR SCAN: {type(exc).__name__}: {exc}", flush=True)

        elapsed = time.time() - started
        time.sleep(max(1.0, SCAN_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
