#!/usr/bin/env python3
"""Tests locaux sans Internet pour vérifier la logique critique."""
import time
import scanner

now = time.time()
pair = "TEST_USDT"
h = scanner.history[pair]
price = 1.0
qv = 1_000_000.0

# 20 minutes stables, environ 1 000 USDT de volume/minute
for i in range(20):
    h.append(scanner.Snapshot(now - (20-i)*60, price, qv + i*1000, 0.999, 1.001))

# Minute actuelle: volume x10 et petit début de hausse
h.append(scanner.Snapshot(now, 1.008, qv + 20_000 + 10_000, 1.007, 1.009))

sig = scanner.score_signal(pair)
assert sig is not None, "Le signal test devrait être détecté"
assert sig.vol_ratio >= 6, sig.vol_ratio
assert sig.score >= scanner.PILOT_SCORE, sig.score

assert scanner.excluded_pair("BTC3L_USDT")
assert scanner.excluded_pair("USD1_USDT")
assert not scanner.excluded_pair("BTC_USDT")

print("SMOKE TEST OK")
print(f"signal={sig.level} score={sig.score} volume=x{sig.vol_ratio:.1f}")
