#!/opt/kronos/venv/bin/python
# Kronos-mini - OBSERVATION SEULEMENT. Aucun ordre, aucun webhook.
import sys, os, time, sqlite3, requests, pandas as pd, torch
sys.path.insert(0, "/opt/kronos/repo")
from model import Kronos, KronosTokenizer, KronosPredictor
torch.set_num_threads(1)
DB = "/opt/kronos/predictions.db"; N = 4; LOOKBACK = 400
TF = {"15m": 900, "1h": 3600}
URL = "https://fapi.bitunix.com/api/v1/futures/market/kline"

def klines(tf, limit=LOOKBACK + 1):
    r = requests.get(URL, params={"symbol": "BTCUSDT", "interval": tf, "limit": limit}, timeout=20)
    d = r.json()["data"]
    df = pd.DataFrame(d)
    for c in ["open", "high", "low", "close", "baseVol", "quoteVol"]: df[c] = df[c].astype(float)
    df["ts"] = pd.to_datetime(df["time"].astype("int64"), unit="ms")
    df = df.sort_values("ts").reset_index(drop=True)
    now = time.time()
    df = df[df["time"].astype("int64") / 1000 + TF[tf] <= now]  # bougies cloturées uniquement
    return df.rename(columns={"baseVol": "volume", "quoteVol": "amount"}).tail(LOOKBACK).reset_index(drop=True)

db = sqlite3.connect(DB)
db.execute("""CREATE TABLE IF NOT EXISTS predictions(id INTEGER PRIMARY KEY, created_at TEXT, timeframe TEXT,
 last_candle TEXT, target_candle TEXT, horizon INTEGER, last_close REAL, pred_close REAL, pred_change_pct REAL,
 pred_direction TEXT, actual_close REAL, actual_change_pct REAL, actual_direction TEXT, correct INTEGER,
 duration_s REAL, UNIQUE(timeframe, last_candle))""")

def evaluate(tf, df):
    m = dict(zip(df["ts"].astype(str), df["close"]))
    for pid, tgt, lc in db.execute("SELECT id,target_candle,last_close FROM predictions WHERE timeframe=? AND correct IS NULL", (tf,)).fetchall():
        if tgt in m:
            a = m[tgt]; ch = (a - lc) / lc * 100; ad = "UP" if a > lc else "DOWN"
            pd_ = db.execute("SELECT pred_direction FROM predictions WHERE id=?", (pid,)).fetchone()[0]
            db.execute("UPDATE predictions SET actual_close=?,actual_change_pct=?,actual_direction=?,correct=? WHERE id=?",
                       (a, ch, ad, int(ad == pd_), pid))
    db.commit()

predictor = None
for tf, sec in TF.items():
    df = klines(tf); evaluate(tf, klines(tf, 200))
    last = str(df["ts"].iloc[-1])
    if db.execute("SELECT 1 FROM predictions WHERE timeframe=? AND last_candle=?", (tf, last)).fetchone(): continue
    if predictor is None:
        tok = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-2k")
        mdl = Kronos.from_pretrained("NeoQuasar/Kronos-mini")
        predictor = KronosPredictor(mdl, tok, device="cpu", max_context=2048)
    t0 = time.time()
    y_ts = pd.Series(pd.date_range(df["ts"].iloc[-1] + pd.Timedelta(seconds=sec), periods=N, freq=f"{sec}s"))
    with torch.no_grad():
        p = predictor.predict(df=df[["open", "high", "low", "close", "volume", "amount"]], x_timestamp=df["ts"],
                              y_timestamp=y_ts, pred_len=N, T=1.0, top_p=0.9, sample_count=1, verbose=False)
    lc = df["close"].iloc[-1]; pc = float(p["close"].iloc[-1]); ch = (pc - lc) / lc * 100
    db.execute("INSERT INTO predictions(created_at,timeframe,last_candle,target_candle,horizon,last_close,pred_close,pred_change_pct,pred_direction,duration_s) VALUES(?,?,?,?,?,?,?,?,?,?)",
               (time.strftime("%Y-%m-%d %H:%M:%S"), tf, last, str(y_ts.iloc[-1]), N, lc, pc, ch, "UP" if pc > lc else "DOWN", time.time() - t0))
    db.commit()
    print(f"{tf} last={last} close={lc} -> +{N} pred={pc:.1f} ({ch:+.3f}%) {time.time()-t0:.1f}s", flush=True)
