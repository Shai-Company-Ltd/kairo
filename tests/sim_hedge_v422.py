#!/usr/bin/env python3
"""Simulation hors-ligne (aucun appel réseau) de l'incident du 23/10 : compte HEDGE, 2 shorts fusionnés
dans un seul positionId, partielle 1R, SYSTEM_CANCELED, SL/TP par trade. Lancer : python3 tests/sim_hedge_v422.py"""
import os, sys, tempfile, time, itertools
os.environ["ENV_FILE"] = "/dev/null"
os.environ["DB_PATH"] = tempfile.mktemp(suffix=".db")
os.environ["WEBHOOK_URL"] = ""
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "server"))
import engine as E
E.REAL_ORDERS = True
E.time.sleep = lambda *_: None   # simulation instantanée

class FakeEx:
    """Bitunix simplifié : 1 position SHORT hedge, ordres TP/SL séparés (id), contraintes 30038/30004,
    SYSTEM_CANCELED des ordres les plus récents quand la somme d'un type dépasse la taille de position."""
    def __init__(s):
        s.pid, s.pos, s.ids = "P1", 0.0, itertools.count(1000)
        s.orders, s.hist, s.log, s.fills, s.errors = {}, [], [], [], []
    def _err(s, code, msg):
        s.errors.append(code); raise E.BitunixError(code, msg, "fake")
    def _sum(s, kind, excl=None):
        return sum(o["qty"] for i, o in s.orders.items() if o["kind"] == kind and i != excl)
    def _end(s, i, st):
        o = s.orders.pop(i); s.hist.insert(0, dict(o, id=i, status=st))
    def _shrink(s):
        if s.pos <= 1e-12:
            s.pos = 0.0
            for i in list(s.orders): s._end(i, "SYSTEM_CANCELED")
            return
        for kind in ("sl", "tp"):
            while s._sum(kind) > s.pos + 1e-12:
                s._end(max(i for i, o in s.orders.items() if o["kind"] == kind), "SYSTEM_CANCELED")
    def attach(s, qty, sl, tp):   # ouverture avec TP/SL attachés (id inconnu du moteur)
        s.pos = round(s.pos + qty, 4)
        for k, p in (("sl", sl), ("tp", tp)): s.orders[str(next(s.ids))] = {"kind": k, "price": p, "qty": qty}
    def tick(s, px):   # déclenchements (short : SL si px>=sl, TP si px<=tp)
        for i, o in sorted(s.orders.items()):
            if i in s.orders and ((o["kind"] == "sl" and px >= o["price"]) or (o["kind"] == "tp" and px <= o["price"])):
                q = min(o["qty"], s.pos); s.pos = round(s.pos - q, 4); s.fills.append((o["kind"], o["price"], q))
                s._end(i, "FILLED"); s._shrink()
    def reduce(s, q):
        s.pos = round(s.pos - q, 4); s.fills.append(("partial", None, q)); s._shrink()
    # --- API utilisée par le moteur
    def _view(s, i, o):
        k = o["kind"]
        return {"id": i, "positionId": s.pid, f"{k}Price": str(o["price"]), f"{k}Qty": str(o["qty"]),
                "slPrice" if k == "tp" else "tpPrice": None, "status": o.get("status")}
    def positions(s, sym=None):
        return [{"positionId": s.pid, "qty": str(s.pos), "side": "SELL"}] if s.pos > 0 else []
    def tpsl_pending(s, sym=None): return [s._view(i, o) for i, o in s.orders.items()]
    def tpsl_history(s, sym=None): return [dict(s._view(h["id"], h), status=h["status"]) for h in s.hist]
    def tpsl_place_one(s, pid, qty, sl=None, tp=None, qprec=4, pprec=1):
        if s.pos <= 0: s._err(30004, "Position does not exist")
        kind, px = ("sl", sl) if sl is not None else ("tp", tp)
        if s._sum(kind) + qty > s.pos + 1e-12: s._err(30038, "TP/SL amount > position size")
        i = str(next(s.ids)); s.orders[i] = {"kind": kind, "price": px, "qty": qty}; s.log.append(("place", kind, px, qty))
        return {"orderId": i}
    def _mod(s, kind, oid, px, qty):
        if s.pos <= 0: s._err(30004, "Position does not exist")
        if oid not in s.orders: s._err(30010, "order not exist")
        if s._sum(kind, oid) + qty > s.pos + 1e-12: s._err(30038, "TP/SL amount > position size")
        s.orders[oid].update(price=px, qty=qty); s.log.append(("modify", kind, px, qty))
    def tpsl_modify_sl(s, oid, sl, qty, *a): s._mod("sl", str(oid), sl, qty)
    def tpsl_modify_tp(s, oid, tp, qty, *a): s._mod("tp", str(oid), tp, qty)
    def tpsl_cancel(s, oid):
        if str(oid) not in s.orders: s._err(30010, "order not exist")
        s._end(str(oid), "CANCELED"); s.log.append(("cancel", oid))
    def close_qty(s, pid, side, qty, prec=4):
        if s.pos <= 0: s._err(30004, "Position does not exist")
        s.reduce(min(qty, s.pos))
    def flash_close(s, pid): s.reduce(s.pos)
    def pending_orders(s, sym=None): return []
    def cancel_orders(s, *a, **k): return {}
    def order_detail(s, cid): return {}

ex = FakeEx()
eng = E.Engine.__new__(E.Engine)
eng.api, eng.db = ex, E.DB(os.environ["DB_PATH"])
eng.alerts = []
eng.urgent = lambda typ, why, key=None, **k: eng.alerts.append((typ, why))
eng.alert = type("A", (), {"send": lambda self, *a, **k: None})()
eng.equity = lambda: 100.0
eng.check_global = lambda eq=None: None
eng.candles, eng.live_1m, eng.mode = {}, None, "LIVE"
eng.min_qty, eng.qty_prec, eng.px_prec = 0.0001, 4, 1
old = E.datetime.fromtimestamp(time.time() - 600, E.timezone.utc).isoformat(timespec="seconds")

def new_trade(qty, entry, sl, tp, pdone=2):
    ex.attach(qty, sl, tp)
    return eng.db.x("INSERT INTO trades(mode,opened_ts,timeframe,setup,side,entry,qty,leverage,margin,sl,tp,risk_usdt,"
                    "status,position_id,max_hold_h,init_sl,orig_qty,partial_done) VALUES('LIVE',?,'5m','t','SHORT',?,?,20,1,?,?,1,"
                    "'OPEN','P1',24,?,?,?)", (old, entry, qty, sl, tp, sl, qty, pdone)).lastrowid
def T(i): return eng._fresh(i)
def ok(c, m): print(("OK  " if c else "FAIL"), m); assert c, m

A = new_trade(0.20, 80000, 81000, 79000)
B = new_trade(0.10, 80200, 80800, 79500)
eng.price = 80100; eng.manage()
ok(T(A)["sl_order_id"] and T(A)["tp_order_id"] and T(B)["sl_order_id"], "ids adoptés pour A et B")
ok(not ex.log, "aucune pose/doublon quand tout est en place")

# partielle de B (0.05) : la position passe à 0.25 -> Bitunix SYSTEM_CANCELED des ordres les plus récents (ceux de B)
ex.reduce(0.05)
eng.after_partial(T(B), 79900, 0.05, 0.05, True)
b = T(B); pend = {o["id"]: o for o in ex.tpsl_pending()}
ok(b["sl_order_id"] in pend and float(pend[b["sl_order_id"]]["slQty"]) == 0.05, "SL de B re-posé à SA qty restante 0.05")
ok(b["tp_order_id"] in pend and float(pend[b["tp_order_id"]]["tpQty"]) == 0.05, "TP de B à 0.05")
ok(T(A)["sl_order_id"] in pend and float(pend[T(A)["sl_order_id"]]["slQty"]) == 0.20, "SL de A intact à 0.20")
eng.db.set("partial_guard", {})

# Bitunix annule le SL de A (SYSTEM_CANCELED) -> re-pose de SON SL à 0.20, TP non doublé
ex._end(T(A)["sl_order_id"], "SYSTEM_CANCELED"); n = len(ex.log)
eng.manage(); eng.manage()
pend = {o["id"]: o for o in ex.tpsl_pending()}
ok(T(A)["sl_order_id"] in pend and float(pend[T(A)["sl_order_id"]]["slQty"]) == 0.20, "SL de A re-posé (0.20)")
ok(len([l for l in ex.log[n:] if l[0] == "place"]) == 1, "une seule pose (pas de doublon TP/SL)")
ok(sum(float(o.get("tpQty") or 0) for o in pend.values()) <= ex.pos + 1e-9, "somme TP <= position")

# prix monte : SL BE de B touché (0.05), A reste ouvert avec ses 0.20
ex.tick(80500); eng.price = 80500; eng.manage()
ok(T(B)["status"] == "CLOSED" and T(B)["close_reason"] == "SL propre exécuté", "B clos par SON SL")
ok(T(A)["status"] == "OPEN" and ex.pos == 0.20, "A ouvert, position 0.20")
ok(len(ex.tpsl_pending()) == 2, "seuls les 2 ordres de A restent")

# prix baisse : TP de A -> position 0 -> réconciliation (pas d'alerte NO_SL)
ex.tick(78900); eng.price = 78900; eng.manage()
ok(T(A)["status"] == "CLOSED" and T(A)["close_reason"] == "TP propre exécuté", "A clos par SON TP")
ok(not [a for a in eng.alerts if a[0] == "no_sl"], f"aucune alerte no_sl ({eng.alerts})")
ok(30038 not in ex.errors and 30004 not in ex.errors, f"aucune erreur 30038/30004 ({ex.errors})")
ok(sum(f[2] for f in ex.fills) == 0.30 or abs(sum(f[2] for f in ex.fills) - 0.30) < 1e-9, "qty fermée totale = 0.30 (pas de double TP)")

# 30004 : ensure sur trade dont la position a disparu -> 'closed', pas d'alerte
C = new_trade(0.10, 80000, 81000, 79000); ex.reduce(0.10)
ok(eng.ensure_tpsl(T(C)) == "closed", "position absente -> closed (réconciliation)")
eng.manage(); ok(T(C)["status"] == "CLOSED" and not [a for a in eng.alerts if a[0] == "no_sl"], "C réconcilié sans NO_SL")

# 30038 couvert : D sans SL propre mais position déjà entièrement couverte -> pas de fermeture marché
D = new_trade(0.10, 80000, 81000, 79000); E1 = new_trade(0.10, 80000, 81100, 79100)
eng.price = 80000; eng.manage()
ex.orders[str(next(ex.ids))] = {"kind": "sl", "price": 81050, "qty": ex.pos - ex._sum("sl")} if ex.pos > ex._sum("sl") else None
ex.orders = {k: v for k, v in ex.orders.items() if v}
sid = T(D)["sl_order_id"]; ex._end(sid, "CANCELED"); ex.orders[str(next(ex.ids))] = {"kind": "sl", "price": 81050, "qty": 0.10}
eng.manage()
ok(T(D)["status"] == "OPEN" and ex.pos == 0.20, "30038/couvert : pas de fermeture au marché")
print("TOUS LES TESTS OK")

# modification EN PLACE (ordres vivants) : même orderId, qty réduite
F = new_trade(0.10, 80000, 81000, 79000, pdone=0); eng.manage(); f = T(F); n = len(ex.log)
ex.reduce(0.0)   # pas de réduction : les ordres de F restent vivants
eng.after_partial(f, 79800, 0.05, 0.05, True); f2 = T(F)
ok(f2["sl_order_id"] == f["sl_order_id"] and ex.orders[f2["sl_order_id"]]["qty"] == 0.05, "SL modifié en place (même id, 0.05)")
ok([l[0] for l in ex.log[n:n+2]] == ["modify", "modify"], "aucune pose/annulation, que des modify")
print("TOUS LES TESTS OK (2)")
