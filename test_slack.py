#!/usr/bin/env python3
"""
Test Slack volontairement minimal:
- 1 seule requête
- aucun retry
- aucune boucle
Usage:
  SLACK_WEBHOOK_URL="..." python test_slack.py
"""
import os
import sys
import requests

url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
if not url:
    print("ERREUR: SLACK_WEBHOOK_URL absente")
    sys.exit(2)

r = requests.post(url, json={"text": "🧪 Crypto Radar Clean — test unique Slack"}, timeout=15)
print(f"HTTP {r.status_code}")
print(r.text[:300])
if r.status_code != 200:
    sys.exit(1)
