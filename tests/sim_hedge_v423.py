#!/usr/bin/env python3
"""v4.2.3 — simulation hors-ligne (aucun appel réseau) de l'incident du 24-25/10 : réponse Bitunix en LISTE
à la pose TP/SL ('list' object has no attribute 'get'), re-pose du TP après partielle, ordres TP orphelins
empilés, boucle de re-pose SL. Lancer : python3 tests/sim_hedge_v423.py"""
import os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
# FakeEx + moteur + helpers de la simulation v4.2.2 (sans ses scénarios)
src = open(os.path.join(HERE, "sim_hedge_v422.py")).read().split("\nA = new_trade(")[0]
exec(compile(src.replace("__file__", repr(os.path.join(HERE, "x.py"))), "sim_v422_base", "exec"))

class ListEx(FakeEx):
    """Variante : place_order renvoie une LISTE ; mode 'ambig' = ordre créé PUIS exception (réseau/parse)."""
    mode = "list"
    def tpsl_place_one(s, *a, **k):
        r = FakeEx.tpsl_place_one(s, *a, **k)
        if s.mode == "ambig":
            raise ValueError("réponse illisible (ordre pourtant créé)")
        if s.mode == "bare":
            return [r["orderId"]]
        return [r] if s.mode == "list" else r

def fresh():
    global ex
    ex = ListEx(); eng.api = ex
    for t in eng.open_trades():
        eng.db.x("UPDATE trades SET status='CLOSED' WHERE id=?", (t["id"],))
    eng.db.set("need_tpsl", []); eng.db.set("partial_guard", {}); eng.db.set("sl_fix_fail", {})
    eng._orphan_seen, eng._orphan_ts = set(), 0
def nt(qty, entry, sl, tp, pdone=2):
    E_ex = ex; E_ex.attach(qty, sl, tp)
    return eng.db.x("INSERT INTO trades(mode,opened_ts,timeframe,setup,side,entry,qty,leverage,margin,sl,tp,risk_usdt,"
                    "status,position_id,max_hold_h,init_sl,orig_qty,partial_done) VALUES('LIVE',?,'5m','t','SHORT',?,?,20,1,?,?,1,"
                    "'OPEN','P1',24,?,?,?)", (old, entry, qty, sl, tp, sl, qty, pdone)).lastrowid
def evs(kind): return [r for r in eng.db.q("SELECT * FROM events WHERE kind=?", (kind,))]
def places(n0, kind=None): return [l for l in ex.log[n0:] if l[0] == "place" and (kind is None or l[1] == kind)]

# 1) parsing : dict, liste, liste de scalaires, imbriqué
R = E.Engine._resp_oid
ok(R({"orderId": "11"}) == "11" and R([{"orderId": 12}]) == "12" and R(["13"]) == "13"
   and R({"data": [{"orderId": "14"}]}) == "14" and R([]) is None and R(None) is None, "orderId lu dans dict ET liste")

# 2) partielle puis re-pose du TP (réponse LISTE) : ordres de B SYSTEM_CANCELED -> re-pose à la qty restante
for mode in ("list", "bare", "ambig"):
    fresh(); ex.mode = mode
    A = nt(0.0721, 80000, 81000, 79000); B = nt(0.0024, 80200, 80800, 79500)
    eng.price = 80100; eng.manage()
    ex.reduce(0.0012)                       # partielle de B -> Bitunix annule les ordres les plus récents (B)
    n0, ev0 = len(ex.log), len(evs("tpsl_placed"))
    eng.after_partial(T(B), 79900, 0.0012, 0.0012, True)
    eng.db.set("partial_guard", {})
    for _ in range(40):                     # ~10 min de boucle de suivi (15 s)
        eng._orphan_ts = 0; eng.manage()
    pend = {o["id"]: o for o in ex.tpsl_pending()}; b = T(B)
    ok(b["tp_order_id"] in pend and float(pend[b["tp_order_id"]]["tpQty"]) == 0.0012, f"[{mode}] TP de B re-posé, id enregistré, qty 0.0012")
    ok(b["sl_order_id"] in pend and float(pend[b["sl_order_id"]]["slQty"]) == 0.0012, f"[{mode}] SL de B à 0.0012")
    ok(len(places(n0, "tp")) == 1, f"[{mode}] une seule pose TP (pas de 33 orphelins) : {len(places(n0, 'tp'))}")
    ok(len(places(n0, "sl")) <= 1, f"[{mode}] SL posé au plus une fois")
    ok(len(pend) == 4, f"[{mode}] exactement 4 ordres (2 par trade) : {len(pend)}")
    ok(T(A)["status"] == "OPEN" and b["status"] == "OPEN", f"[{mode}] A et B toujours ouverts (pas de fermeture marché)")
    ok(not [a for a in eng.alerts if a[0] == "no_sl"], f"[{mode}] aucune alerte no_sl")
    ok(not evs("sl_reposed")[:0] and len([e for e in evs("sl_reposed")]) <= 1, f"[{mode}] pas de boucle sl_reposed")
    eng.alerts.clear(); eng.db.x("DELETE FROM events WHERE kind IN ('sl_reposed','tpsl_covered')")

# 3) orphelins : 33 TP 0.0012 empilés sur la position + 1 SL étranger -> annulés, ceux des trades intacts
fresh(); ex.mode = "list"
A = nt(0.0721, 80000, 81000, 79000); eng.price = 80100; eng.manage()
for _ in range(33):
    ex.orders[str(next(ex.ids))] = {"kind": "tp", "price": 79500.0, "qty": 0.0012}
ex.orders[str(next(ex.ids))] = {"kind": "sl", "price": 80800.0, "qty": 0.0012}
eng._orphan_ts = 0; eng.manage()
ok(len(ex.tpsl_pending()) == 36 and not evs("tpsl_orphan_cancelled"), "1er passage : orphelins seulement repérés (rien annulé)")
eng._orphan_ts = 0; eng.manage()
pend = {o["id"]: o for o in ex.tpsl_pending()}; a = T(A)
ok(set(pend) == {a["sl_order_id"], a["tp_order_id"]}, f"2e passage : seuls SL/TP de A restent ({len(pend)})")
ok(len(evs("tpsl_orphan_cancelled")) == 34, "34 événements tpsl_orphan_cancelled")
ok(float(pend[a["sl_order_id"]]["slQty"]) == 0.0721 and float(pend[a["tp_order_id"]]["tpQty"]) == 0.0721, "A garde SL+TP 0.0721")

# 4) garde-fous : trade récent / partielle en cours -> aucune annulation
fresh(); A = nt(0.0721, 80000, 81000, 79000); eng.price = 80100; eng.manage()
ex.orders[str(next(ex.ids))] = {"kind": "tp", "price": 79500.0, "qty": 0.0012}
eng.db.set("partial_guard", {str(A): time.time() + 45})
ok(eng.cleanup_orphans(force=True) == [], "partielle en cours : pas de nettoyage")
eng.db.set("partial_guard", {}); eng.db.set("need_tpsl", [A])
ok(eng.cleanup_orphans(force=True) == [], "re-pose en attente : pas de nettoyage")
eng.db.set("need_tpsl", [])
ok(len(eng.cleanup_orphans(force=True)) == 1, "sans garde : orphelin annulé")

# 5) SL déjà en place : fix_missing_sl ne re-pose rien et n'émet pas sl_reposed
fresh(); A = nt(0.0721, 80000, 81000, 79000); eng.price = 80100; eng.manage(); n0 = len(ex.log)
for _ in range(10): eng.fix_missing_sl(T(A))
ok(not places(n0) and not evs("sl_reposed"), "SL propre présent : aucune re-pose, aucun sl_reposed")
print("TOUS LES TESTS v4.2.3 OK")
