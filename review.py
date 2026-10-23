#!/usr/bin/env python3
"""trading-v4 review — revue de performance 3 jours (stdlib, DB en lecture seule)."""
import sqlite3, subprocess, os, datetime as dt
from collections import defaultdict

DB = "/opt/trading-v4/server/journal.db"
KDB = "/opt/kronos/predictions.db"
OUT = "/opt/trading-v4/reviews"
DAYS = 3
MIN_N = 8  # échantillon minimum pour classer un setup×TF

def ro(p):
    return sqlite3.connect(f"file:{p}?mode=ro", uri=True)

def reason(r):
    r = (r or "").lower()
    if "temps max" in r: return "temps max"
    if "exchange" in r: return "exchange"
    if "tp" in r: return "TP"
    if "sl" in r: return "SL"
    return r or "?"

def stats(ts):
    p = [t["pnl"] or 0 for t in ts]
    w = [x for x in p if x > 0]; l = [x for x in p if x <= 0]
    gl = -sum(l)
    eq = pk = dd = 0; cur = worst = 0
    for x in p:
        eq += x; pk = max(pk, eq); dd = max(dd, pk - eq)
        cur = cur + x if x <= 0 else 0; worst = min(worst, cur)
    return dict(n=len(p), pnl=sum(p), wr=100 * len(w) / len(p) if p else 0,
                pf=(sum(w) / gl) if gl else float("inf"), aw=sum(w) / len(w) if w else 0,
                al=sum(l) / len(l) if l else 0, dd=dd, streak=worst,
                fees=sum(t["fees"] or 0 for t in ts), gl=gl)

def fpf(x): return "∞" if x == float("inf") else f"{x:.2f}"
def row(k, s): return f"| {k} | {s['n']} | {s['pnl']:+.2f} | {s['wr']:.0f}% | {fpf(s['pf'])} | {s['aw']:+.2f} / {s['al']:+.2f} |"
HDR = "| Clé | N | PnL | WR | PF | gain/perte moy |\n|---|---|---|---|---|---|"

def group(ts, f):
    g = defaultdict(list)
    for t in ts: g[f(t)].append(t)
    return {k: stats(v) for k, v in sorted(g.items())}

def classify(s, total_loss):
    if s["n"] < MIN_N: return "· (n<%d)" % MIN_N
    if (s["pnl"] < 0 and s["wr"] < 40) or (s["pnl"] < 0 and total_loss and -s["pnl"] >= 0.4 * total_loss):
        return "❌ catastrophique"
    if s["pnl"] > 0 and s["pf"] >= 1.2: return "✅ performant"
    return "⚠️ faible"

def kronos():
    out = []
    try:
        cmd = ["/opt/kronos/stats.py"] if os.geteuid() != 0 else ["sudo", "-u", "trader", "/opt/kronos/stats.py"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        out = [l for l in r.stdout.splitlines() if l.strip()][:4]
    except Exception:
        pass
    if not out:
        try:
            for tf, ok, n in ro(KDB).execute("select timeframe,sum(correct),count(correct) from predictions where correct is not null group by 1"):
                out.append(f"{tf}: {ok}/{n} = {100*ok/n:.1f}%")
        except Exception:
            pass
    return out

def main():
    c = ro(DB); c.row_factory = sqlite3.Row
    allt = [dict(r) for r in c.execute("select * from trades where status='CLOSED' order by closed_ts")]
    now = dt.datetime.now(dt.timezone.utc)
    cut = (now - dt.timedelta(days=DAYS)).isoformat()
    rec = [t for t in allt if (t["closed_ts"] or "") >= cut]
    L = [f"# Revue trading-v4 — {now:%Y-%m-%d %H:%M} UTC", ""]
    if not rec:
        L.append("Aucun trade clôturé sur la période."); rec = []
    sR, sA = stats(rec) if rec else None, stats(allt) if allt else None
    L += [f"Période : {DAYS} derniers jours · échantillon min. pour classer : {MIN_N} trades", "", HDR]
    if sR: L.append(row(f"**{DAYS} j**", sR))
    if sA: L.append(row("**historique**", sA))
    if sR:
        L.append(f"\nMax drawdown {sR['dd']:.2f} USDT · pire série de pertes {sR['streak']:+.2f} · frais {sR['fees']:.2f} USDT ({100*sR['fees']/max(sR['gl'],1e-9):.0f}% des pertes brutes)")
        L += ["", "## Par TF / sens / sortie", HDR]
        for lab, f in (("", lambda t: t["timeframe"]), ("", lambda t: t["side"]), ("sortie ", lambda t: reason(t["close_reason"]))):
            for k, s in group(rec, f).items(): L.append(row(lab + str(k), s))
        L += ["", "## Setup × TF", "| Setup×TF | N | PnL | WR | PF | gain/perte moy | Verdict |", "|---|---|---|---|---|---|---|"]
        tl = sR["gl"]; cls = {}
        for k, s in group(rec, lambda t: f"{t['setup']} {t['timeframe']}").items():
            cls[k] = (classify(s, tl), s); L.append(row(k, s) + f" {cls[k][0]} |")
        L.append(f"\nRègles : ❌ = n≥{MIN_N} et PnL<0 avec WR<40% ou ≥40% des pertes ; ✅ = PnL>0 et PF≥1.2 ; sinon ⚠️.")
        # Recommandations
        R = []
        for k, (v, s) in cls.items():
            if v.startswith("❌"): R.append(f"Couper ou mettre en papier **{k}** ({s['pnl']:+.2f}, WR {s['wr']:.0f}%).")
        for k, s in group(rec, lambda t: t["timeframe"]).items():
            if s["n"] >= MIN_N and s["pnl"] < 0 and s["pf"] < 0.8: R.append(f"TF **{k}** déficitaire (PF {fpf(s['pf'])}) : réduire le risque ou le retirer.")
        ex = group(rec, lambda t: reason(t["close_reason"]))
        tm = ex.get("temps max")
        if tm and tm["n"] / sR["n"] >= 0.4:
            R.append(f"{tm['n']}/{sR['n']} sorties au temps max (PnL {tm['pnl']:+.2f}) : revoir `max_hold_h` ou rapprocher le TP (ex. 1.5R au lieu du TP actuel), ou sortie partielle à 1R.")
        sl = ex.get("SL")
        if sl and sR["al"] and abs(sR["al"]) > 1.5 * sR["aw"]: R.append("Perte moyenne > 1.5× gain moyen : tester un trailing stop / break-even à 1R.")
        if sR["fees"] > 0.3 * max(sR["gl"], 1e-9): R.append("Frais élevés vs pertes : privilégier les TF plus longs, entrées post-only.")
        sides = group(rec, lambda t: t["side"])
        for k, s in sides.items():
            if s["n"] >= MIN_N and s["pnl"] < 0: R.append(f"Sens **{k}** perdant : ajouter un filtre de tendance HTF (EMA200 1h/4h) avant entrée.")
        R.append("Indicateurs à tester : filtre de volatilité ATR (éviter les marchés plats → moins de temps max), VWAP/OBV pour confirmer les breakouts, régime ADX par TF, et biais directionnel Kronos 1h comme filtre.")
        L += ["", "## Recommandations"] + [f"- {r}" for r in R[:8]]
    k = kronos()
    if k: L += ["", "## Kronos (précision directionnelle)"] + [f"- {x}" for x in k]
    os.makedirs(OUT, exist_ok=True)
    txt = "\n".join(L) + "\n"
    for p in (f"{OUT}/review-{now:%Y%m%d}.md", f"{OUT}/latest.md"):
        with open(p, "w") as f: f.write(txt)
    print(txt)

if __name__ == "__main__":
    main()
