#!/usr/bin/env python3
"""v4.2.4 — simulation hors-ligne (horloge virtuelle, aucun appel réseau) des délais d'entrée limite :
60 s sans remplissage, 180 s dès un remplissage partiel, jamais d'entrée au marché, suivi SL/TP des autres
trades pendant l'attente (boucle non bloquée), pas de nettoyage d'orphelins pendant l'attente.
Lancer : python3 tests/sim_entry_timeout.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
src = open(os.path.join(HERE, "sim_hedge_v422.py")).read().split("\nA = new_trade(")[0]
exec(compile(src.replace("__file__", repr(os.path.join(HERE, "x.py"))), "sim_v422_base", "exec"))

ok(E.ENTRY_TIMEOUT_SEC == 60 and E.ENTRY_PARTIAL_TIMEOUT_SEC == 180, "défauts 60 s / 180 s")

clock = [time.time()]
E.time.time = lambda: clock[0]
E.time.sleep = lambda s=0: clock.__setitem__(0, clock[0] + s)

class EntryEx(FakeEx):
    """Ordre limite : remplissage programmé {t_rel: qty cumulée}."""
    def __init__(s, sched):
        FakeEx.__init__(s); s.sched, s.t0, s.bodies, s.cancelled, s.st = sched, None, [], False, "NEW"
    def depth(s, sym, n=1): return {"bids": [["80000.0", "1"]], "asks": [["80000.1", "1"]]}
    def place_order(s, body): s.bodies.append(dict(body)); s.t0 = clock[0]; return {"orderId": "X"}
    def _filled(s):
        q = 0.0
        for tr, qq in sorted(s.sched.items()):
            if clock[0] - s.t0 >= tr and (not s.cancelled or tr <= s.cancel_t - s.t0): q = qq
        return q
    def order_detail(s, cid):
        q = s._filled(); full = float(s.bodies[0]["qty"])
        st = "FILLED" if q >= full - 1e-12 else ("CANCELED" if s.cancelled else ("PART_FILLED" if q > 0 else "NEW"))
        return {"status": st, "tradeQty": str(q), "avgPrice": "80000.0"}
    def cancel_orders(s, *a, **k): s.cancelled, s.cancel_t = True, clock[0]; return {}

calls = {"manage": 0, "orphans_pending": 0}
real_manage, real_orph = E.Engine.manage, E.Engine.cleanup_orphans
def spy_manage(self): calls["manage"] += 1; return real_manage(self)
def spy_orph(self, *a, **k):
    if getattr(self, "_entry_pending", False): calls["orphans_pending"] += 1
    return real_orph(self, *a, **k)
E.Engine.manage, E.Engine.cleanup_orphans = spy_manage, spy_orph

P = {"sl": 81000.0, "tp": 79000.0, "qty": 0.0010}
def run(sched):
    global ex
    ex = EntryEx(sched); ex.attach(0.0003, 81000, 79000); eng.api = ex; calls["manage"] = 0; calls["orphans_pending"] = 0
    body = {"symbol": "BTCUSDT", "side": "SELL", "tradeSide": "OPEN", "orderType": "MARKET", "qty": "0.0010", "clientId": "c"}
    t0 = clock[0]; r = eng._entry_limit(body, "SHORT", P, "c")
    return r, clock[0] - t0
def last(kind):
    rows = eng.db.q("SELECT * FROM events WHERE kind=? ORDER BY id DESC LIMIT 1", (kind,))
    return E.json.loads(rows[0]["data"]) if rows and "data" in rows[0].keys() else (rows[0] if rows else None)

# un trade déjà ouvert (SL/TP en place) doit rester suivi pendant l'attente
A = new_trade(0.0003, 80000, 81000, 79000); eng.price = 80050   # position recréée dans chaque EntryEx

# 1) rien de rempli -> annulé à ~60 s
r, dt = run({})
ok(r is None and ex.cancelled and 60 <= dt < 70, f"non rempli : annulé après {dt:.0f} s (60 s)")
ok(all(b["orderType"] == "LIMIT" and b["effect"] == "POST_ONLY" for b in ex.bodies) and len(ex.bodies) == 1,
   "jamais d'entrée au marché (1 seul ordre LIMIT POST_ONLY)")
ok(calls["manage"] >= 5, f"suivi SL/TP des autres trades pendant l'attente ({calls['manage']} passages)")

# 2) partiel à 30 s, jamais complété -> reste annulé à ~180 s, partie remplie conservée
r, dt = run({30: 0.0004})
ok(r is not None and abs(r[1] - 0.0004) < 1e-9 and ex.cancelled and 180 <= dt < 190,
   f"partiel : reste annulé après {dt:.0f} s, qty gardée {r and r[1]}")
ok(calls["manage"] >= 15, f"boucle non bloquée pendant 3 min ({calls['manage']} passages de suivi)")
ok(calls["orphans_pending"] == 0, "aucun nettoyage d'orphelins pendant l'attente d'entrée")

# 3) partiel à 30 s puis complet à 100 s -> trade normal, pas d'annulation
r, dt = run({30: 0.0004, 100: 0.0010})
ok(r is not None and abs(r[1] - 0.0010) < 1e-9 and not ex.cancelled and 100 <= dt < 110,
   f"complété pendant l'attente : qty 0.0010, aucune annulation ({dt:.0f} s)")

# 4) complet tout de suite
r, dt = run({2: 0.0010})
ok(r is not None and r[1] == 0.0010 and dt < 10, "rempli immédiatement : sortie immédiate")

# 5) partiel à 55 s (juste avant 60 s) -> attente prolongée jusqu'à 180 s depuis la pose
r, dt = run({55: 0.0002})
ok(r is not None and abs(r[1] - 0.0002) < 1e-9 and 180 <= dt < 190, f"partiel tardif : délai 180 s depuis la pose ({dt:.0f} s)")

# 6) le trade A existant garde ses SL/TP
a = T(A); pend = {o["id"] for o in ex.tpsl_pending()}
ok(a["status"] == "OPEN", "trade existant toujours ouvert")
ok(a["sl_order_id"] in pend and a["tp_order_id"] in pend, "trade existant garde SL et TP")
ok(not [x for x in eng.alerts if x[0] in ("no_sl", "order_error")], f"aucune alerte : {eng.alerts}")
print("ALL OK")
