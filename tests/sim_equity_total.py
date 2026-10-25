#!/usr/bin/env python3
"""v4.2.6 — simulation hors-ligne (aucun appel réseau) de la fausse alerte du 25/10 15:15 UTC
(« drawdown du jour 26.4% (1000.25 -> 736.12) » à la pose de l'entrée #74) : l'equity doit être
available + margin + frozen + PnL latent ; la taille des positions reste inchangée.
Lancer : python3 tests/sim_equity_total.py"""
import os, sys, types
HERE = os.path.dirname(os.path.abspath(__file__))
src = open(os.path.join(HERE, "sim_hedge_v422.py")).read().split("\nA = new_trade(")[0]
exec(compile(src.replace("__file__", repr(os.path.join(HERE, "x.py"))), "sim_v422_base", "exec"))

class Acct:
    def __init__(s, d): s.d = d
    def account(s): return [dict(s.d)]          # Bitunix renvoie parfois une liste
    def klines(s, *a, **k): return [{"close": "80660.2"}]

eng.equity = types.MethodType(E.Engine.equity, eng)
eng.check_global = types.MethodType(E.Engine.check_global, eng)
eng.alerts.clear()
day = E.datetime.now(E.timezone.utc).strftime("%Y-%m-%d")
eng.db.set("day_start", {"day": day, "equity": 1000.25}); eng.db.set("dd_alert_day", None)

# 1) entrée #74 en carnet : marge gelée dans 'frozen' (cas de l'incident)
pend = {"available": "736.12", "frozen": "247.3", "margin": "0", "crossUnrealizedPNL": "0", "isolationUnrealizedPNL": "0"}
eng.api = Acct(pend)
eq = eng.equity()
ok(abs(eq - 983.42) < 1e-6, f"entrée en attente : equity {eq:.2f} (frozen inclus, plus 736.12)")
ok(abs(E.account_equity(pend, include_frozen=False) - 736.12) < 1e-6, "ancienne formule = 736.12 (bug reproduit)")
eng.price = 80660.2; eng.check_global()
ok(not [a for a in eng.alerts if a[0] == "drawdown"], "aucune alerte drawdown à la pose de l'entrée")

# 2) position ouverte : marge + PnL latent isolé (réponse réelle du 25/10 ~15:50 UTC)
live = {"available": "738.3987", "frozen": "0", "margin": "250.1902", "crossUnrealizedPNL": "0", "isolationUnrealizedPNL": "-0.38619"}
eng.api = Acct(live); eq = eng.equity()
ok(abs(eq - 988.20271) < 1e-4, f"position ouverte : equity {eq:.2f} ≈ 989 ± PnL latent")
ok(abs(eng.db.get("live_equity") - eq) < 1e-9, "live_equity (heartbeat/digest) = equity totale")

# 3) kill switch : se déclenche sur l'equity totale, pas sur available
low = {"available": "2", "frozen": "5", "margin": "6", "isolationUnrealizedPNL": "-0.5"}
ok(E.account_equity(low) == 12.5 and 12.5 > E.KILL_EQUITY, "kill : 2+5+6-0.5 = 12.5 > 12 -> pas de HALT")

# 4) taille INCHANGÉE : plan() utilise toujours available + margin + PnL (sans frozen), clamp sur available
E.LEVERAGE = 20
eng.api = Acct(live)
a = {"close": 80660.2, "atr": 150.0}
p = eng.plan("15m", "LONG", a)
base = E.account_equity(live, include_frozen=False)
sl_dist = max(E.ATR_SL_MULT * 150.0, 80660.2 * E.MIN_SL_PCT / 100)
q = min(base * E.RISK_PCT / 100 / sl_dist, base * E.MAX_MARGIN_PCT / 100 * 20 / 80660.2,
        738.3987 * 0.95 * 20 / 80660.2 / (1 + E.FEE_RATE * 20))
ok(p["qty"] == E.round_down(q, 4), f"taille identique à v4.2.5 : qty {p['qty']}")
eng.api = Acct(dict(live, frozen="100"))
ok(eng.plan("15m", "LONG", a)["qty"] == p["qty"], "frozen n'influence pas la taille")
ok(abs(eng.sizing_equity() - base) < 1e-9 and abs(eng.equity() - base - 100) < 1e-9, "sizing_equity = ancienne base")
print("ALL OK")
