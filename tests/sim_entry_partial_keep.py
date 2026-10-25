#!/usr/bin/env python3
"""v4.2.5 — simulation hors-ligne (horloge virtuelle, aucun appel réseau) de l'entrée partielle conservée
(ENTRY_PARTIAL_KEEP=1) : reste de l'ordre limite laissé en carnet, ajouté au MÊME trade à chaque remplissage,
annulé au 1er TP (partielle 1R), au SL / à la clôture, ou au kill switch ; boucle jamais bloquée ; état persistant
(redémarrage) ; orphelins jamais annulés sur une entrée en cours ; jamais d'ordre au marché.
Lancer : python3 tests/sim_entry_partial_keep.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
src = open(os.path.join(HERE, "sim_hedge_v422.py")).read().split("\nA = new_trade(")[0]
exec(compile(src.replace("__file__", repr(os.path.join(HERE, "x.py"))), "sim_v422_base", "exec"))

E.ENTRY_PARTIAL_KEEP = True; E.ENTRY_POST_ONLY = True; E.PARTIAL_TP = True; E.PARTIAL_LIMIT = True
ok(E.ENTRY_TIMEOUT_SEC == 60, "délai sans remplissage : 60 s")
clock = [time.time()]
E.time.time = lambda: clock[0]
E.time.sleep = lambda s=0: clock.__setitem__(0, clock[0] + s)
def adv(s): clock[0] += s

class KeepEx(FakeEx):
    """Ordres limites (entrée + partielle) par clientId ; chaque remplissage d'entrée attache SES TP/SL (pire cas)."""
    def __init__(s):
        FakeEx.__init__(s); s.lim, s.bodies, s.cancels = {}, [], []
    def depth(s, sym, n=1): return {"bids": [["80000.0", "1"]], "asks": [["80000.1", "1"]]}
    def place_order(s, body):
        s.bodies.append(dict(body))
        s.lim[body["clientId"]] = {"body": dict(body), "filled": 0.0, "st": "NEW"}
        return {"orderId": "L" + body["clientId"]}
    def fill(s, cid, q):   # remplissage (cumul q) de l'ordre limite
        o = s.lim[cid]; add = round(q - o["filled"], 4); o["filled"] = q
        o["st"] = "FILLED" if q >= float(o["body"]["qty"]) - 1e-12 else "PART_FILLED"
        if o["body"]["tradeSide"] == "OPEN":
            s.attach(add, float(o["body"]["slPrice"]), float(o["body"]["tpPrice"]))
        else:
            s.reduce(add)
    def order_detail(s, cid):
        o = s.lim.get(cid)
        if not o: return {}
        return {"status": o["st"], "tradeQty": str(o["filled"]), "avgPrice": o["body"]["price"]}
    def cancel_orders(s, sym, order_ids=(), client_ids=()):
        for c in client_ids:
            s.cancels.append(c)
            if c in s.lim and s.lim[c]["st"] in ("NEW", "PART_FILLED"): s.lim[c]["st"] = "CANCELED"
        return {}
    def live(s, cid): return s.lim[cid]["st"] in ("NEW", "PART_FILLED")

def setup():
    global ex
    ex = KeepEx(); eng.api = ex
    for t in eng.open_trades(): eng.db.x("UPDATE trades SET status='CLOSED', pnl=0, entry_pending=0 WHERE id=?", (t["id"],))
    eng.db.x("UPDATE trades SET entry_pending=0, pnl=COALESCE(pnl,0)")
    for k, v in (("need_tpsl", []), ("partial_guard", {}), ("sl_fix_fail", {}), ("entry_orders", {}), ("halted", None)):
        eng.db.set(k, v)
    eng._orphan_seen, eng._orphan_ts = set(), 0
    eng.price = 80050
SIG = {"side": "SHORT", "setup": "s1", "confidence": 0.8, "why": "test"}
P = {"price": 80000.1, "sl": 81000.0, "tp": 78000.0, "qty": 0.0010, "margin": 4.0, "risk_usdt": 1.0}
def open_(sig=SIG):
    t0 = clock[0]; r = eng.open_trade("15m", sig, dict(P), None)
    return r, clock[0] - t0, ex.bodies[-1]["clientId"] if ex.bodies else None
def trade_of(cid):
    r = eng.db.q("SELECT * FROM trades WHERE client_id=?", (cid,)); return r[0] if r else None
def backdate(tid, s=600):
    ts = E.datetime.fromtimestamp(clock[0] - s, E.timezone.utc).isoformat(timespec="seconds")
    eng.db.x("UPDATE trades SET opened_ts=? WHERE id=?", (ts, tid))
def own(t, k): o = ex.orders.get(str(t[f"{k}_order_id"])); return o and o["qty"]
def no_market():
    return all(b["orderType"] == "LIMIT" for b in ex.bodies)

# 0) non bloquant + rien rempli à 60 s -> annulé
setup(); calls = {"manage": 0}
rm = E.Engine.manage
def spy(self): calls["manage"] += 1; return rm(self)
E.Engine.manage = spy
r, dt, cid = open_()
ok(r is True and dt < 5, f"open_trade rend la main tout de suite ({dt:.1f} s)")
ok(cid in eng.entry_orders(), "ordre d'entrée enregistré en état persistant (kv entry_orders)")
for _ in range(3): adv(15); eng.poll_entries(); eng.manage()
ok(calls["manage"] == 3 and not trade_of(cid), "boucle libre : 3 tours de suivi pendant l'attente, pas de trade")
ok(eng.entry_dup("15m", "s1", "SHORT") is not None, "même setup en attente -> pas de 2e ordre")
adv(20); eng.poll_entries()
ok(cid in ex.cancels and not eng.entry_orders() and not trade_of(cid), "non rempli à 60 s : annulé, aucun trade")
E.Engine.manage = rm

# 1) partiel puis 1R (partielle) -> reste annulé
setup(); r, dt, cid = open_()
adv(10); ex.fill(cid, 0.0004); eng.poll_entries(); t = trade_of(cid)
ok(t and t["qty"] == 0.0004 and t["entry_pending"] == 1, "partiel : trade créé à 0.0004, reste en carnet")
ok(own(t, "sl") == 0.0004 and own(t, "tp") == 0.0004, "SL/TP propres à la qty remplie")
adv(300); eng.poll_entries()
ok(ex.live(cid) and cid not in ex.cancels, "reste toujours en carnet après 5 min (plus de délai 180 s)")
ok(eng.entry_dup("15m", "s1", "SHORT") is not None, "reste en carnet -> pas de 2e ordre identique")
ok(eng.can_open("SHORT", "15m") is not None or E.MAX_PER_TF > 1, "le trade compte pour les limites de positions")
backdate(t["id"]); t = trade_of(cid); pc = t["partial_cid"]
ok(pc and float(ex.lim[pc]["body"]["qty"]) == 0.0002, "partielle limite 1R posée sur la qty remplie (0.0002)")
ex.fill(pc, 0.0002); eng.price = 79000; eng.manage(); t = trade_of(cid)
ok(t["partial_done"] == 1 and t["entry_pending"] == 0 and cid in ex.cancels and not ex.live(cid),
   "1R touché : reste d'entrée annulé")
ok(t["status"] == "OPEN" and t["qty"] == 0.0002 and own(t, "sl") == 0.0002, "le trade continue (0.0002, SL BE)")
ok(no_market(), "aucun ordre au marché")

# 2) partiel puis SL -> reste annulé
setup(); r, dt, cid = open_(dict(SIG, setup="s2"))
adv(5); ex.fill(cid, 0.0005); eng.poll_entries(); t = trade_of(cid); backdate(t["id"])
ex.lim[t["partial_cid"]]["st"] = "NEW"
ex.tick(81000); eng.price = 81000; eng.manage(); t = trade_of(cid)
ok(t["status"] == "CLOSED" and "SL" in t["close_reason"], f"SL exécuté : trade clos ({t['close_reason']})")
ok(t["entry_pending"] == 0 and cid in ex.cancels and not ex.live(cid), "SL : reste d'entrée annulé")
ok(no_market(), "aucun ordre au marché")

# 3) remplissage supplémentaire -> SL/TP (et partielle) redimensionnés, même trade, mêmes prix
setup(); r, dt, cid = open_(dict(SIG, setup="s3"))
adv(5); ex.fill(cid, 0.0004); eng.poll_entries(); t = trade_of(cid); sl_id, tp_id = t["sl_order_id"], t["tp_order_id"]
adv(30); ex.fill(cid, 0.0008); eng.poll_entries(); t = trade_of(cid)
ok(t["qty"] == 0.0008 and t["orig_qty"] == 0.0008 and t["entry_filled"] == 0.0008, "qty/orig_qty -> 0.0008 (même trade)")
ok(len(eng.db.q("SELECT id FROM trades WHERE client_id=?", (cid,))) == 1, "aucun 2e trade créé")
ok(t["sl_order_id"] == sl_id and own(t, "sl") == 0.0008 and own(t, "tp") == 0.0008,
   "SL/TP propres redimensionnés EN PLACE à 0.0008 (même orderId)")
ok(ex.orders[sl_id]["price"] == 81000.0 and ex.orders[tp_id]["price"] == 78000.0, "mêmes prix SL/TP")
ok(ex._sum("sl") == ex.pos and ex._sum("tp") == ex.pos, "aucun doublon : somme SL = somme TP = position")
ok(float(ex.lim[t["partial_cid"]]["body"]["qty"]) == 0.0004, "partielle limite redimensionnée (0.0004)")
adv(30); ex.fill(cid, 0.0010); eng.poll_entries(); t = trade_of(cid)
ok(t["qty"] == 0.0010 and t["entry_pending"] == 0 and own(t, "sl") == 0.0010, "complété : 0.0010, suivi du reste terminé")
ok(no_market() and not [a for a in eng.alerts if a[0] in ("no_sl", "order_error")], f"aucune alerte ({eng.alerts})")

# 4) redémarrage : avant remplissage et avec un reste en carnet
setup(); r, dt, cidA = open_(dict(SIG, setup="s4a"))
eng2 = E.Engine.__new__(E.Engine); eng2.__dict__.update({k: v for k, v in eng.__dict__.items() if k != "db"})
eng2.db = E.DB(os.environ["DB_PATH"])
ok(cidA in eng2.entry_orders(), "redémarrage : ordre d'entrée retrouvé")
adv(5); ex.fill(cidA, 0.0003); eng2.poll_entries(); t = trade_of(cidA)
ok(t and t["qty"] == 0.0003 and t["entry_pending"] == 1, "redémarrage : remplissage repris, trade créé")
eng3 = E.Engine.__new__(E.Engine); eng3.__dict__.update(eng2.__dict__); eng3.db = E.DB(os.environ["DB_PATH"])
adv(15); ex.fill(cidA, 0.0006); eng3.poll_entries(); t = trade_of(cidA)
ok(t["qty"] == 0.0006 and own(t, "sl") == 0.0006, "redémarrage : reste en carnet repris, remplissage ajouté")
eng.db.set("entry_orders", {cidA: {"tf": "15m"}}); eng3.poll_entries()   # trade déjà créé : pas de doublon
ok(len(eng.db.q("SELECT id FROM trades WHERE client_id=?", (cidA,))) == 1 and not eng.entry_orders(),
   "redémarrage : idempotent (pas de 2e trade)")

# 5) orphelins : ni le reste, ni les SL/TP d'une entrée en cours
setup(); r, dt, cid = open_(dict(SIG, setup="s5"))
adv(5); ex.fill(cid, 0.0004); eng.poll_entries(); t = trade_of(cid); backdate(t["id"])
ex.attach(0.0002, 81000.0, 78000.0)   # TP/SL attachés à un remplissage pas encore vu par le moteur
n = len(ex.log); c = eng.cleanup_orphans(force=True); c += eng.cleanup_orphans(force=True)
ok(not c and not [l for l in ex.log[n:] if l[0] == "cancel"], "aucun TP/SL annulé sur une position avec reste en carnet")
ok(ex.live(cid) and cid not in ex.cancels, "le reste de l'ordre d'entrée n'est pas annulé")
r, dt, cid2 = open_(dict(SIG, setup="s5b"))
ok(eng.cleanup_orphans(force=True) == [], "entrée sans trade encore en attente : nettoyage suspendu")

# 6) kill switch : tout est annulé
E.KILL_EQUITY, kq = 1e9, E.KILL_EQUITY
E.Engine.check_global(eng, 50.0)
ok(not ex.live(cid) and not ex.live(cid2) and not eng.entry_orders(), "kill switch : reste + entrée en attente annulés")
ok(trade_of(cid)["status"] == "CLOSED" and trade_of(cid)["entry_pending"] == 0, "kill switch : trade clos, plus de reste")
E.KILL_EQUITY = kq
ok(no_market(), "jamais d'ordre au marché (entrée/partielle = LIMIT uniquement)")
ok(all(b.get("effect") == "POST_ONLY" for b in ex.bodies if b["tradeSide"] == "OPEN"), "entrées POST_ONLY")

# 7) repli 4.2.4 : ENTRY_PARTIAL_KEEP=0 -> chemin bloquant historique
setup(); E.ENTRY_PARTIAL_KEEP = False
called = []
eng._entry_limit = lambda *a: called.append(1) or None
eng.open_trade("15m", dict(SIG, setup="s7"), dict(P), None)
ok(called and not eng.entry_orders(), "ENTRY_PARTIAL_KEEP=0 : comportement 4.2.4 (_entry_limit)")
print("ALL OK")
