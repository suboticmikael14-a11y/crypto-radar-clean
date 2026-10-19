#!/usr/bin/env python3
"""Pépito V10 observational shadow.

Runs on the SEPARATE Railway test service only. Never sends Slack, places orders,
or changes V9B production. V9B-eligible signals are the comparison cohort; a
pre-registered V10 rule flags unlimited qualified candidates per KST day, prospectively. Stores both
chosen AND unchosen signals in the test service's persistent SQLite volume.

Evaluation labels (fixed BEFORE live observation):
- "pump10"/"pump20": any minute-sampled *best bid* reaches >=10%/20%
  above the simulated $200 buy VWAP in the following 24h.
- "net_at_1h/4h/24h": observed quoted best bid / entry VWAP - 1 - 1.3pp
  conservative cost proxy. NOT a realizable P&L or executable sell quote.
- Compare hit rate (precision) AND number of pumps caught (recall); include
  missed pumps, false positives, signal time differences and volume.
- Clock: absolute UTC timestamps; daily selection bucket: Asia/Seoul (UTC+9).
- Honest limitations: V10 baseline approximates V9B notifications on an
  independent scanner, without V8 AI/portfolio state; official prod Slack is
  the canonical V9B baseline for final comparison. Missing minute scans may
  miss highs. Books are snapshots and can disappear.
"""
import json
import math
import os
import sqlite3
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import scanner as v9

# Fail closed irrespective of deployment variables.
v9.SLACK_ENABLED = False
v9.SLACK_WEBHOOK_URL = ""
v9.AI_ENABLED = False
v9.OPENAI_API_KEY = ""
v9.STATE_DB_PATH = os.getenv("V10_SCANNER_DB", "/data/v10_scanner.sqlite3")
v9.POSITIONS_FILE = "/data/v10_no_positions.json"
DB_PATH = os.getenv("V10_SHADOW_DB", "/data/v10_shadow.sqlite3")
RULE_VERSION = "V10_IMPROVED_20261019"
SELECT_THRESHOLD = 68
MAX_DAILY_SELECTIONS = 1000000
HORIZONS_MIN = (60, 240, 1440)
COST_PROXY_PCT = 1.30
KST = timezone(timedelta(hours=9))


def score_signal(item, candle, book):
    """0..100, uses information available BEFORE any subsequent movement."""
    r1 = v9.fnum(candle.get("r1"))
    r5 = v9.fnum(candle.get("r5"))
    r15 = v9.fnum(candle.get("r15"))
    r24 = v9.fnum(item.get("change_percentage"))
    v5 = v9.fnum(candle.get("volume_5m_usd_est"))
    q24 = v9.fnum(item.get("quote_volume"))
    cross = v9.fnum(book.get("cross_pct"), 100)
    # Momentum 0..30. Do not demand huge 24h gains to trigger early.
    momentum = min(15.0, max(0.0, r5) * 2.5)
    momentum += min(10.0, max(0.0, r15) * 1.3)
    momentum += min(5.0, max(0.0, r1) * 2.5)
    # 5m trading activity and 24h depth: 0..25
    recent = max(0.0, min(15.0, 5.0 + 8.0 * math.log10(max(v5, 1.0) / 300.0)))
    daily = max(0.0, min(10.0, 6.0 + 4.0 * math.log10(max(q24, 1.0) / 10000.0)))
    volume = recent + daily
    # Real $200 two-sided orderbook execution expense, 0..25
    execution = max(0.0, 25.0 - 8.0 * cross)
    # Early-stage potential vs already up strongly in 24h, 0..20.
    if r24 <= -15:
        early = 10.0
    elif r24 < -5:
        early = 15.0
    elif r24 <= 12:
        early = 20.0
    else:
        early = max(0.0, 20.0 - 0.45 * (r24 - 12))
    # V10: dynamic market-phase score, no cap on daily signals.
    phase_bonus = (6 if r1 > 0.1 and r5 > 0.5 and r15 > 0.7
                   else 3 if abs(r5) < 1 and v5 > 600
                   else -5 if r5 > 12 and r1 < 0 else 0)
    return round(max(0.0, min(100.0, momentum + volume + execution + early + phase_bonus)), 1)


def selftest():
    good = {"change_percentage":9.69,"quote_volume":243846}
    gc = {"r1":2.14,"r5":6.60,"r15":4.14,"volume_5m_usd_est":6106.93}
    gb = {"cross_pct":0.9}
    bad = {"change_percentage":40,"quote_volume":1000}
    bc = {"r1":0,"r5":3.01,"r15":0.6,"volume_5m_usd_est":301}
    bb = {"cross_pct":2.4}
    assert score_signal(good,gc,gb) > score_signal(bad,bc,bb)
    assert score_signal(good,gc,gb) >= SELECT_THRESHOLD
    assert all(0 <= score_signal(good,gc,{"cross_pct":x}) <= 100 for x in (0,1,2.5,500))
    assert MAX_DAILY_SELECTIONS > 1000 and COST_PROXY_PCT >= 1.0
    print("V10 SELFTEST PASS | ranking monotonic | fixed threshold | no Slack | no orders",flush=True)


def setup_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".",exist_ok=True)
    db=sqlite3.connect(DB_PATH,timeout=20)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""CREATE TABLE IF NOT EXISTS shadow_signals(
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       created_at REAL NOT NULL, kst_day TEXT NOT NULL, pair TEXT NOT NULL,
       instrument TEXT NOT NULL, version TEXT NOT NULL,
       chosen INTEGER NOT NULL, score REAL NOT NULL,
       last_price REAL NOT NULL, best_ask REAL NOT NULL, best_bid REAL NOT NULL,
       buy_vwap REAL NOT NULL, book_cross_pct REAL NOT NULL,
       r1 REAL NOT NULL,r5 REAL NOT NULL,r15 REAL NOT NULL,r24 REAL NOT NULL,
       v5_usd REAL NOT NULL,q24_usd REAL NOT NULL,
       highest_bid REAL NOT NULL,lowest_bid REAL NOT NULL,
       samples INTEGER NOT NULL DEFAULT 0)""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_v10_time ON shadow_signals(created_at)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_v10_day ON shadow_signals(kst_day,chosen)")
    db.execute("""CREATE TABLE IF NOT EXISTS v10_followup(
       event_id INTEGER NOT NULL,observed_at REAL NOT NULL,bid REAL NOT NULL,
       gross_pct REAL NOT NULL,phase TEXT NOT NULL,PRIMARY KEY(event_id,observed_at))""")
    db.execute("""CREATE TABLE IF NOT EXISTS v10_exit_simulations(
       event_id INTEGER NOT NULL,strategy TEXT NOT NULL,net_proxy_pct REAL NOT NULL,
       reason TEXT NOT NULL,PRIMARY KEY(event_id,strategy))""")
    db.execute("""CREATE TABLE IF NOT EXISTS shadow_outcomes(
       event_id INTEGER NOT NULL,horizon_min INTEGER NOT NULL,
       observed_at REAL NOT NULL, bid REAL NOT NULL,
       return_after_cost_pct REAL NOT NULL,highest_bid REAL NOT NULL,
       lowest_bid REAL NOT NULL, samples INTEGER NOT NULL,
       PRIMARY KEY(event_id,horizon_min))""")
    db.execute("""CREATE TABLE IF NOT EXISTS shadow_runs(
       tick_time REAL PRIMARY KEY, scanned INTEGER NOT NULL,
       v9b_candidates INTEGER NOT NULL, verified INTEGER NOT NULL,
       selected INTEGER NOT NULL, send_slack INTEGER NOT NULL DEFAULT 0)""")
    db.commit()
    return db


def recorded_day_count(db,day):
    return db.execute("SELECT COUNT(*) FROM shadow_signals WHERE kst_day=? AND chosen=1",(day,)).fetchone()[0]


def record(db,item,candle,book,now):
    day=datetime.fromtimestamp(now,KST).strftime("%Y-%m-%d")
    score=score_signal(item,candle,book)
    chosen=(score >= SELECT_THRESHOLD) # Unlimited daily candidates; quality threshold only
    pair=item["currency_pair"]
    ask=v9.fnum(item.get("lowest_ask"))
    bid=v9.fnum(item.get("highest_bid"))
    entry=v9.fnum(book.get("avg_buy"))
    if min(ask,bid,entry)<=0:
        return None
    db.execute("""INSERT INTO shadow_signals(
      created_at,kst_day,pair,instrument,version,chosen,score,last_price,
      best_ask,best_bid,buy_vwap,book_cross_pct,r1,r5,r15,r24,
      v5_usd,q24_usd,highest_bid,lowest_bid,samples)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
      now,day,pair,item.get("exchange_symbol",""),RULE_VERSION,int(chosen),score,
      v9.fnum(item.get("last")),ask,bid,entry,v9.fnum(book.get("cross_pct")),
      v9.fnum(candle.get("r1")),v9.fnum(candle.get("r5")),
      v9.fnum(candle.get("r15")),v9.fnum(item.get("change_percentage")),
      v9.fnum(candle.get("volume_5m_usd_est")),v9.fnum(item.get("quote_volume")),
      bid,bid,1))
    db.commit()
    print("V10 SHADOW SIGNAL "+json.dumps({
      "pair":pair,"kst_day":day,"score":score,"chosen":bool(chosen),
      "entry_buy_vwap":round(entry,10),"book_cross_pct":round(book["cross_pct"],2),
      "v5_usd":round(v9.fnum(candle.get("volume_5m_usd_est")),2),
      "r5":round(v9.fnum(candle.get("r5")),2)},separators=(",",":")),flush=True)
    return chosen


def update_outcomes(db,tickers_by_pair,now):
    pending=db.execute("""SELECT id,created_at,pair,buy_vwap,highest_bid,lowest_bid,samples
       FROM shadow_signals WHERE created_at>=?""",(now-26*3600,)).fetchall()
    sample_count=0
    completed=0
    for id_,created,pair,entry,high,low,n in pending:
        # Do not count the detection scan as a post-alert return.
        if now<=created+3 or now>created+24*3600+180:
            continue
        item=tickers_by_pair.get(pair)
        if not item:
            continue
        bid=v9.fnum(item.get("highest_bid"))
        if bid<=0:
            continue
        gross=100.0*(bid/entry-1.0)
        phase=("ACCELERATING" if gross>100*(high/entry-1) else
               "REVERSING" if gross<100*(high/entry-1)-5 else "CONSOLIDATING")
        db.execute("INSERT OR IGNORE INTO v10_followup(event_id,observed_at,bid,gross_pct,phase) VALUES(?,?,?,?,?)",
                   (id_,now,bid,gross,phase))
        high=max(high,bid)
        low=min(low,bid)
        n+=1
        db.execute("UPDATE shadow_signals SET highest_bid=?,lowest_bid=?,samples=? WHERE id=?",
                   (high,low,n,id_))
        sample_count+=1
        for horizon in HORIZONS_MIN:
            if now < created+horizon*60:
                continue
            # The FIRST scan after the horizon is the designated exit quote.
            result=db.execute("SELECT 1 FROM shadow_outcomes WHERE event_id=? AND horizon_min=?",
                              (id_,horizon)).fetchone()
            if result:
                continue
            net=100.0*(bid/entry-1.0)-COST_PROXY_PCT
            db.execute("""INSERT INTO shadow_outcomes(event_id,horizon_min,observed_at,
                bid,return_after_cost_pct,highest_bid,lowest_bid,samples)
                VALUES(?,?,?,?,?,?,?,?)""",(id_,horizon,now,bid,net,high,low,n))
            completed+=1
            if horizon==1440:
                simulate_exits(db,id_,entry)
            print("V10 SHADOW OUTCOME "+json.dumps({
                "id":id_,"pair":pair,"horizon_min":horizon,
                "net_proxy_pct":round(net,2),
                "max_quoted_gain_pct":round(100*(high/entry-1),2),
                "max_quoted_drawdown_pct":round(100*(low/entry-1),2),
                "sampled_minutes":n},separators=(",",":")),flush=True)
    db.commit()
    return sample_count,completed



def simulate_exits(db,event_id,entry):
    """Prospective rules evaluated on observed bid snapshots; not executable P&L."""
    rows=db.execute("SELECT observed_at,bid FROM v10_followup WHERE event_id=? ORDER BY observed_at",(event_id,)).fetchall()
    if not rows or entry<=0:return
    start=rows[0][0]
    for name,target,stop,trail,minutes in (
        ("tp10_stop5",10,-5,False,1440),
        ("tp20_stop8",20,-8,False,1440),
        ("trailing5_stop8",None,-8,True,1440),
        ("time4h",None,None,False,240),
        ("time24h",None,None,False,1440)):
        peak=-1000;out=None;reason="last_sample"
        for ts,bid in rows:
            gain=100*(bid/entry-1)
            peak=max(peak,gain)
            if stop is not None and gain<=stop:out=gain;reason="stop";break
            if target is not None and gain>=target:out=gain;reason="target";break
            if trail and peak>=5 and gain<=peak-5:out=gain;reason="trailing";break
            if ts-start>=minutes*60:out=gain;reason="time";break
        if out is None:out=100*(rows[-1][1]/entry-1)
        db.execute("INSERT OR REPLACE INTO v10_exit_simulations VALUES(?,?,?,?)",
                   (event_id,name,round(out-COST_PROXY_PCT,4),reason))

def missed_pump_report(db):
    """Same 24h cohort, including rejected signals, without hindsight in selection."""
    rows=db.execute("""SELECT s.chosen,s.pair,s.score,
         100*(o.highest_bid/s.buy_vwap-1) AS peak
         FROM shadow_signals s JOIN shadow_outcomes o ON o.event_id=s.id
         WHERE o.horizon_min=1440""").fetchall()
    missed=sorted([(p,round(g,2),sc) for chosen,p,sc,g in rows if not chosen and g>=20],
                  key=lambda x:-x[1])
    hits=sum(1 for chosen,p,sc,g in rows if chosen and g>=20)
    return {"missed20":missed[:20],"captured20":hits,
            "total20":hits+len([1 for chosen,p,sc,g in rows if not chosen and g>=20]),
            "calibration":"unavailable until sufficient independent labeled samples"}

def report(db):
    total,selected=db.execute("SELECT COUNT(*),COALESCE(SUM(chosen),0) FROM shadow_signals").fetchone()
    outcomes=db.execute("""SELECT s.chosen,o.return_after_cost_pct,o.highest_bid/s.buy_vwap
       FROM shadow_signals s JOIN shadow_outcomes o ON s.id=o.event_id
       WHERE o.horizon_min=1440""").fetchall()
    def group(chosen):
        vals=[x for x in outcomes if x[0]==chosen]
        hits10=sum(x[2]>=1.10 for x in vals)
        hits20=sum(x[2]>=1.20 for x in vals)
        return {"matured":len(vals),"pumps10":hits10,"pumps20":hits20,
                "avg_24h_net_proxy_pct":round(sum(x[1] for x in vals)/len(vals),2) if vals else None}
    latest=db.execute("SELECT MAX(created_at),MIN(created_at) FROM shadow_signals").fetchone()
    print("V10 SHADOW REPORT "+json.dumps({"rule":RULE_VERSION,"signals":total,
          "v10_selected":selected,"start":latest[1],"latest":latest[0],
          "v10":group(1),"v9b_other":group(0),"missed_pumps":missed_pump_report(db)},separators=(",",":")),flush=True)


def run():
    selftest()
    db=setup_db()
    v9.init_state_db()
    v9.restore_pilots(time.time())
    v9.restore_pilot_history(time.time())
    print("V10 SHADOW ACTIVE | V9B algorithm reference | SLACK OFF | AI OFF "
          "| LIVE ORDERS NONE | own isolated persistent SQLite | "
          f"rule={RULE_VERSION} | selected daily cap=NONE "
          f"| threshold={SELECT_THRESHOLD}",flush=True)
    counter=0
    while True:
        start=time.time()
        try:
            tickers=v9.fetch_tickers()
            now=time.time()
            market={x["currency_pair"]:x for x in tickers}
            for item in tickers:
                v9.add_snapshot(item,now)
            v9.commit_state(now)
            samples,outcomes=update_outcomes(db,market,now)
            chosen=v9.v5_choose_candle_pairs(tickers)
            candles=v9.v5_load_candles(chosen,now)
            ready=[]
            for pair,candle in candles.items():
                item=market.get(pair)
                if not item:
                    continue
                if v9.speculative_confirm(pair,item,candle,now):
                    ready.append((pair,item,candle))
            ready.sort(key=lambda x:(min(15.0,v9.fnum(x[2].get("r5")))
                    +max(0.0,min(5.0,v9.fnum(x[2].get("r1")))),
                    v9.fnum(x[1].get("quote_volume"))),reverse=True)
            candidates=[]
            for pair,item,candle in ready:
                if v9.speculative_notification_precheck(item,candle) is None:
                    candidates.append((pair,item,candle))
            hourly=sum(1 for ts in v9.last_spec_alert_at.values() if ts>=now-3600)
            slots=max(0,min(v9.SPEC_ALERTS_PER_SCAN,v9.SPEC_ALERTS_PER_HOUR-hourly))
            books_checked=0
            verified=0
            selected=0
            for pair,item,candle in candidates:
                if verified>=slots or books_checked>=v9.SPEC_BOOK_CHECKS_PER_SCAN:
                    break
                latest_candle=dict(candle)
                latest_candle["age_sec"]=time.time()-v9.fnum(candle.get("closed_at"))-60
                if latest_candle["age_sec"]>v9.CANDLE_MIN_FRESH_SEC:
                    continue
                live=v9.speculative_live_requote(pair,item)
                if not live or v9.speculative_notification_precheck(live,latest_candle):
                    continue
                books_checked+=1
                book=v9.speculative_fetch_book(pair,live)
                if book is None:
                    continue
                verified+=1
                picked=record(db,live,candle,book,time.time())
                # In production V9B marks a pair only after sending Slack; in this
                # reference shadow, mark the WOULD-BE send, with no network send.
                if picked is not None:
                    v9.mark_spec_sent(pair,time.time())
                    selected+=int(picked)
            db.execute("INSERT OR REPLACE INTO shadow_runs(tick_time,scanned,v9b_candidates,verified,selected,send_slack) VALUES(?,?,?,?,?,0)",
                       (now,len(candles),len(candidates),verified,selected))
            db.commit()
            print(f"V10 SHADOW SCAN | candles={len(candles)} candidate={len(candidates)}"
                  f" v9b_equivalent={verified} v10_picks={selected}"
                  f" outcome_samples={samples} horizons_completed={outcomes} "
                  "| Slack=OFF | orders=NONE",flush=True)
            counter+=1
            if counter % 30==1:
                report(db)
        except Exception as exc:
            print(f"V10 SHADOW ERROR | {type(exc).__name__}: {exc}",flush=True)
        time.sleep(max(1.0,v9.SCAN_INTERVAL-(time.time()-start)))


if __name__=="__main__":
    if os.getenv("V10_SELFTEST_ONLY","0") in ("1","true","yes"):
        selftest()
    elif os.getenv("V10_REPORT_ONLY","0") in ("1","true","yes"):
        report(setup_db())
    else:
        run()
