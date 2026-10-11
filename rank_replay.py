#!/usr/bin/env python3
"""Read-only, exploratory forward-return audit of actual Slack spot alerts.
No Slack sends, orders, credentials, or production imports. Never treat highs as executable returns.
Inputs: PEPITO_REPLAY_ALERTS JSON: {symbol, instrument, ts_utc, last, ask, r1, r5, r15, r24, vol5, qv24, spread, cross}.
"""
import json
import os
import time
import statistics
from datetime import datetime, timezone
import requests

URL = "https://api.crypto.com/exchange/v1/public/get-candlestick"
ALERTS = json.loads(os.environ.get("PEPITO_REPLAY_ALERTS", "[]"))
FEES_BUFFER_PCT = 1.30
HORIZONS = (15, 60, 240)
session = requests.Session()

def numeric(x, default=0.0):
    try:
        return float(str(x).replace(",","").replace("+",""))
    except (ValueError, TypeError):
        return default

def rank_at_alert(a):
    """Fixed baseline rule derived from visible market concepts. NOT trained or validated."""
    r1, r5, r15, r24 = (numeric(a.get(k)) for k in ("r1","r5","r15","r24"))
    v5, vol24, spread = (numeric(a.get(k)) for k in ("vol5","qv24","spread"))
    # Weighted maximum 100: 30 momentum, 25 volume, 25 spread, 20 early-stage.
    momentum = (14 if r5 >= 5 else 9 if r5 >= 3 else 3 if r5 >= 1 else 0)
    momentum += 10 if r15 >= 5 else 6 if r15 >= 2 else 2 if r15 >= 0.5 else 0
    momentum += 6 if r1 >= 1 else 3 if r1 > 0 else 0
    volume = (16 if v5 >= 5000 else 13 if v5 >= 2500 else 10 if v5 >= 1000
              else 6 if v5 >= 300 else 2 if v5 >= 100 else 0)
    volume += 9 if vol24 >= 100000 else 6 if vol24 >= 25000 else 2 if vol24 >= 5000 else 0
    execution = 25 if spread <= 0.2 else 21 if spread <= 0.5 else 16 if spread <= 1 else 11 if spread <= 2 else 0
    early = 20 if 0 <= r24 < 12 else 15 if -10 <= r24 < 20 else 8 if r24 < 35 else 2 if r24 < 50 else 0
    return momentum + volume + execution + early

def candles(a,now):
    ts = datetime.strptime(a["ts_utc"],"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    # Ask for complete closed five-minute candles after the Slack notification.
    start=(int(ts)//300+1)*300
    end=min(now-300,ts+240*60+300)
    if end < start+300:
        return ts,[]
    params={"instrument_name":a["instrument"],"timeframe":"5m",
            "start_ts":int((start-300)*1000),"end_ts":int(end*1000),
            "count":100}
    try:
        r=session.get(URL,params=params,timeout=12)
        r.raise_for_status()
        body=r.json()
        if body.get("code")!=0:
            print("API_ERROR",a["symbol"],body.get("code"),body.get("message"),flush=True)
            return ts,[]
        items=body.get("result",{}).get("data",[])
        data=sorted(({"t":numeric(q.get("t"))/1000.0,"o":numeric(q.get("o")),
                       "h":numeric(q.get("h")),"l":numeric(q.get("l")),
                       "c":numeric(q.get("c")),"v":numeric(q.get("v"))}
                    for q in items),key=lambda q:q["t"])
        data=[x for x in data if x["t"]>=start and x["t"]+300 <= now and x["h"]>0 and x["c"]>0]
        return ts,data
    except Exception as exc:
        print("API_EXCEPTION",a["symbol"],type(exc).__name__,str(exc)[:140],flush=True)
        return ts,[]

def audit(a,now):
    timestamp,rows=candles(a,now)
    ask=numeric(a.get("ask"))
    if ask<=0:
        ask=numeric(a["last"])*(1+numeric(a.get("spread"))/200)
    result={"symbol":a["symbol"],"time":a["ts_utc"],"score":rank_at_alert(a),
            "ask":ask,"r24_before":numeric(a.get("r24")),"n":len(rows)}
    for h in HORIZONS:
        if now < timestamp+h*60+300:
            result[f"{h}m"]="pending"
            continue
        relevant=[x for x in rows if x["t"]+300 <= timestamp+h*60 and x["t"]>=timestamp]
        if not relevant or ask<=0:
            result[f"{h}m"]="no_candles"
            continue
        # Not executable bids: market mid close estimate and hindsight high/low only.
        close=relevant[-1]["c"]
        high=max(x["h"] for x in relevant)
        low=min(x["l"] for x in relevant)
        gross=(close/ask-1)*100
        # Approximated *not executable* return using the INITIAL spread as future exit proxy
        # + baseline round-trip fee/buffer; never call realized P&L.
        net_proxy=gross-FEES_BUFFER_PCT-numeric(a.get("spread"))
        result[f"{h}m"]={"gross_close_pct":round(gross,2),
                        "net_proxy_pct":round(net_proxy,2),
                        "high_pct":round((high/ask-1)*100,2),
                        "low_pct":round((low/ask-1)*100,2),
                        "candles":len(relevant)}
    return result

def main():
    assert ALERTS, "Missing replay alert dataset"
    now=time.time()
    rows=[]
    for a in ALERTS:
        if not a.get("instrument","").endswith(("_USD","_USDT")):
            continue
        rows.append(audit(a,now))
        time.sleep(0.1)
    print("RANK_REPLAY_V1 COUNT",len(rows),"historical, alerts-only, selection bias present",flush=True)
    for a in sorted(rows,key=lambda x:x["score"],reverse=True):
        print("RANK_RESULT",json.dumps(a,ensure_ascii=False,sort_keys=True),flush=True)
    valid=[x for x in rows if isinstance(x.get("240m"),dict)]
    ranked=sorted(valid,key=lambda x:x["score"],reverse=True)
    if len(ranked)>=5:
        groups={"top3":ranked[:3],"others":ranked[3:]}
        for name,data in groups.items():
            print("RANK_GROUP",name,"count",len(data),
                  "avg4h_net_proxy",round(statistics.mean(x["240m"]["net_proxy_pct"] for x in data),2),
                  "positive4h",sum(x["240m"]["net_proxy_pct"]>0 for x in data),flush=True)
    print("RANK_REPLAY_COMPLETE",flush=True)

if __name__=="__main__":
    main()
