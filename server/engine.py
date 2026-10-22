#!/usr/bin/env python3
"""Moteur trading v4 — BTCUSDT Bitunix USDT-M, x10 isolé, multi-timeframes.

Zéro token LLM : il surveille le marché, gère le risque, journalise en SQLite
et ne réveille les agents Grok Bot (webhook) que sur un événement notable.

Par défaut : DRY_RUN (paper). Ordres réels seulement si LIVE=1 ET DRY_RUN=0.
Signature Bitunix reprise du projet "competition" (bitunix.py).

Usage :
  python engine.py                 # boucle 24/7
  python engine.py --once          # un seul passage (debug)
  python engine.py --test-webhook  # envoie une alerte de test
  python engine.py --status        # résumé du journal
"""
import os, sys, time, json, math, hashlib, secrets, sqlite3, signal, logging, threading, argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import requests

# ---------------------------------------------------------------- config
def _load_env_file(path):
    if not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if " #" in v:
            v = v.split(" #", 1)[0]
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

_load_env_file(os.environ.get("ENV_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")))

def E(k, d, cast=str):
    v = os.environ.get(k)
    if v is None or v == "":
        return d
    if cast is bool:
        return v.strip().lower() in ("1", "true", "yes", "on")
    return cast(v)

SYMBOL = E("SYMBOL", "BTCUSDT")
LEVERAGE = E("LEVERAGE", 10, int)
TIMEFRAMES = [t.strip() for t in E("TIMEFRAMES", "1m,5m,15m,1h,4h,1d").split(",") if t.strip()]
TRADE_TIMEFRAMES = [t.strip() for t in E("TRADE_TIMEFRAMES", ",".join(TIMEFRAMES)).split(",") if t.strip()]
DRY_RUN = E("DRY_RUN", True, bool)
LIVE = E("LIVE", False, bool)
REAL_ORDERS = LIVE and not DRY_RUN
AUTO_TRADE = E("AUTO_TRADE", True, bool)          # le moteur ouvre lui-même les setups >= MIN_CONF_TRADE
MIN_CONF_TRADE = E("MIN_CONF_TRADE", 0.65, float)
MIN_CONF_ALERT = E("MIN_CONF_ALERT", 0.55, float)
START_EQUITY = E("START_EQUITY", 25.0, float)     # paper
RISK_PCT = E("RISK_PCT", 4.0, float)              # % equity risqué par trade (perte au SL)
MAX_MARGIN_PCT = E("MAX_MARGIN_PCT", 90.0, float) # % equity max en marge par position
MAX_OPEN = E("MAX_OPEN", 2, int)
DAILY_STOP_PCT = E("DAILY_STOP_PCT", 15.0, float) # perte jour (UTC) -> plus de nouveaux trades
KILL_EQUITY = E("KILL_EQUITY", 12.0, float)       # equity <= -> HALT total
TARGET_EQUITY = E("TARGET_EQUITY", 100.0, float)
MAX_CONSEC_LOSSES = E("MAX_CONSEC_LOSSES", 3, int)
PAUSE_AFTER_LOSSES_MIN = E("PAUSE_AFTER_LOSSES_MIN", 120, int)
RR = E("RR", 2.0, float)
ATR_SL_MULT = E("ATR_SL_MULT", 1.5, float)
MIN_SL_PCT = E("MIN_SL_PCT", 0.25, float)         # SL mini en % du prix (anti-bruit + frais)
FEE_RATE = E("FEE_RATE", 0.0006, float)           # taker par côté
MAX_FEE_SHARE = E("MAX_FEE_SHARE", 0.30, float)   # frais aller-retour max / risque
POLL_SEC = E("POLL_SEC", 15, int)
DB_PATH = E("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "journal.db"))
LOG_LEVEL = E("LOG_LEVEL", "INFO")

WEBHOOK_URL = E("WEBHOOK_URL", "")
WEBHOOK_KEY = E("WEBHOOK_KEY", "")
WEBHOOK_KEY_HEADER = E("WEBHOOK_KEY_HEADER", "Authorization: Bearer")
WEBHOOK_TIMEOUT = E("WEBHOOK_TIMEOUT", 10, int)
MAX_ALERTS_DAY = E("MAX_ALERTS_DAY", 10, int)     # alertes "setup" par jour UTC
MAX_EVENTS_DAY = E("MAX_EVENTS_DAY", 12, int)     # trade_open/close/risk par jour
ALERT_MIN_GAP_MIN = E("ALERT_MIN_GAP_MIN", 30, int)
ALERT_COOLDOWN_MIN = E("ALERT_COOLDOWN_MIN", 120, int)  # même (timeframe, setup, sens)
DIGEST_HOURS_UTC = [int(h) for h in E("DIGEST_HOURS_UTC", "6,18").split(",") if h.strip() != ""]
CONTROL_PORT = E("CONTROL_PORT", 0, int)
CONTROL_TOKEN = E("CONTROL_TOKEN", "")
CONTROL_BIND = E("CONTROL_BIND", "127.0.0.1")

TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400, "1d": 86400}
TF_REFRESH = {"1m": 15, "5m": 30, "15m": 60, "30m": 90, "1h": 120, "2h": 180, "4h": 300, "1d": 600}
TF_MAX_HOLD_H = {"1m": 1, "5m": 3, "15m": 8, "30m": 12, "1h": 24, "4h": 72, "1d": 120}
TF_WEIGHT = {"1m": 0.0, "5m": 0.03, "15m": 0.05, "1h": 0.08, "4h": 0.1, "1d": 0.1}

logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("v4")

# ---------------------------------------------------------------- Bitunix client
BASE = "https://fapi.bitunix.com"

class BitunixError(Exception):
    def __init__(self, code, msg, path=""):
        super().__init__(f"{path} code={code} msg={msg}")
        self.code, self.msg = code, msg

class Bitunix:
    """Client REST. Clés lues dans l'env uniquement, jamais loggées."""
    def __init__(self, min_interval=0.15, timeout=15):
        self._key = os.environ.get("BITUNIX_API_KEY", "")
        self._sec = os.environ.get("BITUNIX_API_SECRET", "")
        self.s = requests.Session()
        self.min_interval, self.timeout, self._last = min_interval, timeout, 0.0
        self._lock = threading.Lock()

    def has_keys(self):
        return bool(self._key and self._sec)

    def _throttle(self):
        with self._lock:
            dt = time.time() - self._last
            if dt < self.min_interval:
                time.sleep(self.min_interval - dt)
            self._last = time.time()

    def _sign(self, nonce, ts, qconcat, body):
        d = hashlib.sha256((nonce + ts + self._key + qconcat + body).encode()).hexdigest()
        return hashlib.sha256((d + self._sec).encode()).hexdigest()

    def request(self, method, path, params=None, body=None, private=False, retries=3):
        params = {k: v for k, v in (params or {}).items() if v is not None}
        last = None
        for attempt in range(retries):
            self._throttle()
            try:
                h = {"Content-Type": "application/json", "language": "en-US"}
                body_s = json.dumps(body, separators=(",", ":")) if body is not None else ""
                if private:
                    nonce, ts = secrets.token_hex(16), str(int(time.time() * 1000))
                    qc = "".join(f"{k}{params[k]}" for k in sorted(params))
                    h.update({"api-key": self._key, "nonce": nonce, "timestamp": ts,
                              "sign": self._sign(nonce, ts, qc, body_s)})
                if method == "GET":
                    r = self.s.get(BASE + path, params=params, headers=h, timeout=self.timeout)
                else:
                    r = self.s.post(BASE + path, data=body_s, headers=h, timeout=self.timeout)
                if r.status_code == 429 or r.status_code >= 500:
                    last = BitunixError(r.status_code, r.text[:200], path)
                    time.sleep(1.5 * (attempt + 1)); continue
                j = r.json()
                if j.get("code") != 0:
                    raise BitunixError(j.get("code"), j.get("msg"), path)
                return j.get("data")
            except BitunixError:
                raise
            except (requests.RequestException, ValueError) as e:
                last = e
                time.sleep(1.5 * (attempt + 1))
        raise last if last else RuntimeError("request failed")

    # public
    def klines(self, symbol, interval, limit=200, end=None):
        return self.request("GET", "/api/v1/futures/market/kline",
                            {"symbol": symbol, "interval": interval, "limit": limit, "endTime": end,
                             "type": "LAST_PRICE"})
    def funding(self, symbol):
        d = self.request("GET", "/api/v1/futures/market/funding_rate", {"symbol": symbol})
        return d[0] if isinstance(d, list) else d
    def pair(self, symbol):
        d = self.request("GET", "/api/v1/futures/market/trading_pairs", {"symbols": symbol})
        return d[0] if isinstance(d, list) and d else d
    # private
    def account(self):
        return self.request("GET", "/api/v1/futures/account", {"marginCoin": "USDT"}, private=True)
    def positions(self, symbol=None):
        return self.request("GET", "/api/v1/futures/position/get_pending_positions",
                            {"symbol": symbol}, private=True) or []
    def history_positions(self, symbol=None, limit=20):
        return self.request("GET", "/api/v1/futures/position/get_history_positions",
                            {"symbol": symbol, "limit": limit}, private=True)
    def change_leverage(self, symbol, lev):
        return self.request("POST", "/api/v1/futures/account/change_leverage",
                            body={"symbol": symbol, "leverage": int(lev), "marginCoin": "USDT"}, private=True)
    def change_margin_mode(self, symbol, mode="ISOLATION"):
        return self.request("POST", "/api/v1/futures/account/change_margin_mode",
                            body={"marginMode": mode, "symbol": symbol, "marginCoin": "USDT"}, private=True)
    def place_order(self, body):
        return self.request("POST", "/api/v1/futures/trade/place_order", body=body, private=True, retries=1)
    def order_detail(self, client_id):
        return self.request("GET", "/api/v1/futures/trade/get_order_detail", {"clientId": client_id},
                            private=True, retries=2)
    def close_qty(self, position_id, side, qty, prec=4):
        """Fermeture PARTIELLE (hedge) : side = sens de la position (BUY=long)."""
        return self.request("POST", "/api/v1/futures/trade/place_order",
                            body={"symbol": SYMBOL, "side": "BUY" if side == "LONG" else "SELL",
                                  "tradeSide": "CLOSE", "positionId": str(position_id),
                                  "qty": fmt(qty, prec), "orderType": "MARKET"}, private=True, retries=1)
    def tpsl_pending(self, symbol=None):
        d = self.request("GET", "/api/v1/futures/tpsl/get_pending_orders", {"symbol": symbol}, private=True)
        return d if isinstance(d, list) else (d or {}).get("orderList", []) if isinstance(d, dict) else []
    def tpsl_cancel(self, order_id):
        return self.request("POST", "/api/v1/futures/tpsl/cancel_order",
                            body={"symbol": SYMBOL, "orderId": str(order_id)}, private=True, retries=1)
    def flash_close(self, position_id):
        return self.request("POST", "/api/v1/futures/trade/flash_close_position",
                            body={"positionId": str(position_id)}, private=True, retries=1)

# ---------------------------------------------------------------- indicators
def ema(xs, n):
    if len(xs) < n:
        return [None] * len(xs)
    k, out = 2 / (n + 1), [None] * (n - 1)
    e = sum(xs[:n]) / n
    out.append(e)
    for x in xs[n:]:
        e = x * k + e * (1 - k); out.append(e)
    return out

def rsi(cl, n=14):
    if len(cl) <= n:
        return None
    g = l = 0.0
    for i in range(1, n + 1):
        d = cl[i] - cl[i - 1]; g += max(d, 0); l += max(-d, 0)
    g, l = g / n, l / n
    for i in range(n + 1, len(cl)):
        d = cl[i] - cl[i - 1]
        g = (g * (n - 1) + max(d, 0)) / n; l = (l * (n - 1) + max(-d, 0)) / n
    return 100.0 if l == 0 else 100 - 100 / (1 + g / l)

def atr(c, n=14):
    if len(c) <= n:
        return None
    trs = [max(c[i]["h"] - c[i]["l"], abs(c[i]["h"] - c[i - 1]["c"]), abs(c[i]["l"] - c[i - 1]["c"]))
           for i in range(1, len(c))]
    a = sum(trs[:n]) / n
    for t in trs[n:]:
        a = (a * (n - 1) + t) / n
    return a

def vwap(c, n=None):
    seg = c[-n:] if n else c
    v = sum(x["v"] for x in seg)
    return sum((x["h"] + x["l"] + x["c"]) / 3 * x["v"] for x in seg) / v if v else None

def session_vwap(c):
    """VWAP ancré au début du jour UTC (pour les TF intraday)."""
    if not c:
        return None
    day0 = (c[-1]["t"] // 86400000) * 86400000
    seg = [x for x in c if x["t"] >= day0]
    return vwap(seg) if seg else None

def analyse(c, tf="1h"):
    """c = bougies CLÔTURÉES, ordre chronologique."""
    cl = [x["c"] for x in c]
    e20, e50, e200 = ema(cl, 20), ema(cl, 50), ema(cl, 200)
    vols = [x["v"] for x in c]
    vavg = sum(vols[-21:-1]) / 20 if len(vols) > 21 else None
    hi20 = max(x["h"] for x in c[-21:-1]) if len(c) > 21 else None
    lo20 = min(x["l"] for x in c[-21:-1]) if len(c) > 21 else None
    last = c[-1]
    trend = 0
    if e50[-1] and e200[-1]:
        if last["c"] > e50[-1] > e200[-1]: trend = 1
        elif last["c"] < e50[-1] < e200[-1]: trend = -1
    elif e20[-1] and e50[-1]:
        trend = 1 if e20[-1] > e50[-1] else -1
    return {"close": last["c"], "high": last["h"], "low": last["l"], "open": last["o"], "t": last["t"],
            "ema20": e20[-1], "ema50": e50[-1], "ema200": e200[-1],
            "ema20_prev": e20[-2] if len(e20) > 1 else None,
            "rsi": rsi(cl), "rsi_prev": rsi(cl[:-1]), "atr": atr(c),
            "vwap": session_vwap(c) if TF_SEC.get(tf, 0) <= 3600 else None,
            "vwap50": vwap(c, 50), "vol": last["v"], "vol_avg": vavg,
            "vol_ratio": (last["v"] / vavg) if vavg else None,
            "hi20": hi20, "lo20": lo20, "trend": trend}

# ---------------------------------------------------------------- setups
def detect(tf, a, ctx):
    """Retourne une liste de setups {setup, side, confidence, why}. ctx = analyses des autres TF."""
    out = []
    if not a["atr"] or not a["ema20"] or a["rsi"] is None:
        return out
    px, vr = a["close"], a["vol_ratio"] or 0
    htf = [k for k in ("1h", "4h", "1d") if k in ctx and k != tf]
    bias = sum(ctx[k]["trend"] for k in htf)

    def conf(base, side):
        c = base + (0.08 if vr >= 2 else 0.04 if vr >= 1.5 else 0)
        c += 0.05 * (bias * side if bias * side > 0 else 0) / max(len(htf), 1) * len(htf)
        if bias * side < 0: c -= 0.06 * abs(bias)
        c += TF_WEIGHT.get(tf, 0)
        if a["vwap"]:
            c += 0.03 if (px - a["vwap"]) * side > 0 else -0.03
        return round(max(0.0, min(0.95, c)), 2)

    # 1) cassure du range 20 bougies avec volume
    if a["hi20"] and px > a["hi20"] and vr >= 1.5 and a["trend"] >= 0:
        out.append({"setup": "breakout", "side": "LONG", "confidence": conf(0.55, 1),
                    "why": f"close>{a['hi20']:.1f} (haut 20), vol x{vr:.1f}"})
    if a["lo20"] and px < a["lo20"] and vr >= 1.5 and a["trend"] <= 0:
        out.append({"setup": "breakout", "side": "SHORT", "confidence": conf(0.55, -1),
                    "why": f"close<{a['lo20']:.1f} (bas 20), vol x{vr:.1f}"})
    # 2) pullback sur EMA20 dans la tendance
    if a["trend"] == 1 and a["low"] <= a["ema20"] < px and 40 <= a["rsi"] <= 60 and px > a["open"]:
        out.append({"setup": "pullback_ema20", "side": "LONG", "confidence": conf(0.52, 1),
                    "why": f"tendance haussière, rebond EMA20 {a['ema20']:.1f}, RSI {a['rsi']:.0f}"})
    if a["trend"] == -1 and a["high"] >= a["ema20"] > px and 40 <= a["rsi"] <= 60 and px < a["open"]:
        out.append({"setup": "pullback_ema20", "side": "SHORT", "confidence": conf(0.52, -1),
                    "why": f"tendance baissière, rejet EMA20 {a['ema20']:.1f}, RSI {a['rsi']:.0f}"})
    # 3) excès RSI + spike de volume + bougie de retournement (contre-tendance, confiance plus basse)
    if a["rsi_prev"] is not None:
        if a["rsi_prev"] < 25 and a["rsi"] > a["rsi_prev"] and vr >= 2 and px > a["open"]:
            out.append({"setup": "rsi_reversal", "side": "LONG", "confidence": conf(0.45, 1),
                        "why": f"RSI {a['rsi_prev']:.0f}->{a['rsi']:.0f}, capitulation vol x{vr:.1f}"})
        if a["rsi_prev"] > 75 and a["rsi"] < a["rsi_prev"] and vr >= 2 and px < a["open"]:
            out.append({"setup": "rsi_reversal", "side": "SHORT", "confidence": conf(0.45, -1),
                        "why": f"RSI {a['rsi_prev']:.0f}->{a['rsi']:.0f}, euphorie vol x{vr:.1f}"})
    # 4) spike de volume pur (info, pas de trade)
    if vr >= 4:
        side = "LONG" if px > a["open"] else "SHORT"
        out.append({"setup": "volume_spike", "side": side, "confidence": 0.5, "info_only": True,
                    "why": f"volume x{vr:.1f} la moyenne, bougie {'verte' if side == 'LONG' else 'rouge'}"})
    return out

# ---------------------------------------------------------------- journal
SCHEMA = """
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, ts TEXT, level TEXT, kind TEXT, data TEXT);
CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY, ts TEXT, timeframe TEXT, setup TEXT, side TEXT,
  price REAL, sl REAL, tp REAL, confidence REAL, why TEXT, acted INTEGER DEFAULT 0, skip_reason TEXT);
CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY, mode TEXT, opened_ts TEXT, closed_ts TEXT,
  timeframe TEXT, setup TEXT, side TEXT, entry REAL, qty REAL, leverage INTEGER, margin REAL, sl REAL, tp REAL,
  risk_usdt REAL, exit REAL, pnl REAL, fees REAL, status TEXT, close_reason TEXT, client_id TEXT,
  position_id TEXT, signal_id INTEGER, confidence REAL, max_hold_h REAL);
CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY, ts TEXT, type TEXT, key TEXT, payload TEXT,
  sent INTEGER, http_status INTEGER, suppressed_reason TEXT);
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
"""

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

class DB:
    def __init__(self, path):
        self.c = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.c.execute("PRAGMA journal_mode=WAL")
        self.c.executescript(SCHEMA)
        self.lock = threading.Lock()
    def x(self, q, a=()):
        with self.lock:
            return self.c.execute(q, a)
    def q(self, q, a=()):
        with self.lock:
            cur = self.c.execute(q, a)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    def ev(self, kind, data=None, level="INFO"):
        self.x("INSERT INTO events(ts,level,kind,data) VALUES(?,?,?,?)",
               (now_iso(), level, kind, json.dumps(data or {}, default=str)))
        getattr(log, level.lower(), log.info)("%s %s", kind, json.dumps(data or {}, default=str)[:400])
    def get(self, k, d=None):
        r = self.q("SELECT v FROM kv WHERE k=?", (k,))
        return json.loads(r[0]["v"]) if r else d
    def set(self, k, v):
        self.x("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, json.dumps(v)))

# ---------------------------------------------------------------- webhook
def parse_key_header(spec, key):
    """'Authorization: Bearer' -> ('Authorization','Bearer <key>') ; 'X-Api-Key' -> ('X-Api-Key','<key>')."""
    spec = (spec or "Authorization: Bearer").strip()
    if ":" in spec:
        name, prefix = spec.split(":", 1)
        prefix = prefix.strip()
        return name.strip(), (f"{prefix} {key}" if prefix else key)
    return spec, key

class Alerter:
    PRIORITY = {"trade_open", "trade_close", "risk", "kill_switch", "target", "test", "digest", "startup"}

    ALWAYS = {"trade_open", "trade_close", "kill_switch"}

    def __init__(self, db):
        self.db = db
        self.s = requests.Session()

    def _today(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def send(self, payload, key=None, force=False):
        typ = payload.get("type", "alert")
        payload = {"source": "trading-v4", "symbol": SYMBOL, "ts": now_iso(),
                   "mode": "LIVE" if REAL_ORDERS else "PAPER", **payload}
        day = self._today()
        reason = None
        if typ in self.ALWAYS:   # jamais plafonnés
            force = True
        if not force:
            if typ in self.PRIORITY:
                n = self.db.q("SELECT COUNT(*) n FROM alerts WHERE sent=1 AND substr(ts,1,10)=? AND type IN "
                              "('trade_open','trade_close','risk')", (day,))[0]["n"]
                if typ in ("trade_open", "trade_close", "risk") and n >= MAX_EVENTS_DAY:
                    reason = f"cap events/jour {MAX_EVENTS_DAY}"
            else:
                n = self.db.q("SELECT COUNT(*) n FROM alerts WHERE sent=1 AND substr(ts,1,10)=? AND type='setup'",
                              (day,))[0]["n"]
                last = self.db.q("SELECT ts FROM alerts WHERE sent=1 AND type='setup' ORDER BY id DESC LIMIT 1")
                lastk = self.db.q("SELECT ts FROM alerts WHERE sent=1 AND key=? ORDER BY id DESC LIMIT 1", (key,)) if key else []
                t = time.time()
                if n >= MAX_ALERTS_DAY:
                    reason = f"cap alertes/jour {MAX_ALERTS_DAY}"
                elif lastk and t - datetime.fromisoformat(lastk[0]["ts"]).timestamp() < ALERT_COOLDOWN_MIN * 60:
                    reason = f"cooldown {key}"
                elif last and t - datetime.fromisoformat(last[0]["ts"]).timestamp() < ALERT_MIN_GAP_MIN * 60:
                    reason = f"écart min {ALERT_MIN_GAP_MIN} min"
        if reason:
            self.db.x("INSERT INTO alerts(ts,type,key,payload,sent,suppressed_reason) VALUES(?,?,?,?,0,?)",
                      (now_iso(), typ, key, json.dumps(payload), reason))
            log.info("alerte supprimée (%s): %s %s", reason, typ, key)
            return False
        status = None
        if WEBHOOK_URL:
            h = {"Content-Type": "application/json"}
            if WEBHOOK_KEY:
                hn, hv = parse_key_header(WEBHOOK_KEY_HEADER, WEBHOOK_KEY)
                h[hn] = hv
            for attempt in range(3):   # 1 essai + 2 retries (connexion / 5xx)
                try:
                    r = self.s.post(WEBHOOK_URL, json=payload, headers=h, timeout=WEBHOOK_TIMEOUT)
                    status = r.status_code
                    if status >= 300:
                        log.warning("webhook HTTP %s: %s", status, r.text[:200])
                    if status < 500:
                        break
                except requests.RequestException as e:
                    log.warning("webhook erreur (essai %d): %s", attempt + 1, e)
                    status = -1
                    self.s = requests.Session()   # connexion keep-alive morte -> nouvelle session
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
        else:
            log.info("WEBHOOK_URL vide -> alerte seulement loggée")
        ok = status is None or 200 <= status < 300
        self.db.x("INSERT INTO alerts(ts,type,key,payload,sent,http_status) VALUES(?,?,?,?,?,?)",
                  (now_iso(), typ, key, json.dumps(payload), 1, status))
        log.info("ALERTE %s %s -> %s", typ, json.dumps(payload)[:300], status)
        return ok

# ---------------------------------------------------------------- engine
def fmt(x, prec):
    return f"{x:.{prec}f}"

def round_down(x, prec):
    f = 10 ** prec
    return math.floor(x * f + 1e-9) / f

class Engine:
    def __init__(self):
        self.api = Bitunix()
        self.db = DB(DB_PATH)
        self.alert = Alerter(self.db)
        self.candles, self.last_fetch, self.last_closed, self.cache = {}, {}, {}, {}
        self.ctx, self.funding, self.last_funding_fetch = {}, None, 0
        self.price = None
        self.running = True
        self.min_qty, self.qty_prec, self.px_prec = 0.0001, 4, 1
        self.mode = "LIVE" if REAL_ORDERS else "PAPER"
        if self.db.get("paper_equity") is None:
            self.db.set("paper_equity", START_EQUITY)
        if LIVE and not DRY_RUN and not self.api.has_keys():
            raise SystemExit("LIVE=1 et DRY_RUN=0 mais BITUNIX_API_KEY/SECRET absents.")

    # -- state
    def equity(self):
        if REAL_ORDERS:
            try:
                a = self.api.account()
                a = a[0] if isinstance(a, list) else a
                eq = float(a.get("available", 0)) + float(a.get("margin", 0)) + \
                     float(a.get("crossUnrealizedPNL", 0) or 0) + float(a.get("isolationUnrealizedPNL", 0) or 0)
                self.db.set("live_equity", eq)
                return eq
            except Exception as e:
                log.warning("account(): %s", e)
                return self.db.get("live_equity", START_EQUITY)
        eq = self.db.get("paper_equity", START_EQUITY)
        for t in self.open_trades():   # + PnL latent
            if self.price:
                eq += (1 if t["side"] == "LONG" else -1) * (self.price - t["entry"]) * t["qty"]
        return eq

    def realized_equity(self):
        return self.db.get("paper_equity", START_EQUITY) if not REAL_ORDERS else self.equity()

    def open_trades(self):
        return self.db.q("SELECT * FROM trades WHERE status='OPEN' AND mode=?", (self.mode,))

    def day_start_equity(self):
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        d = self.db.get("day_start", {})
        if d.get("day") != day:
            d = {"day": day, "equity": self.realized_equity()}
            self.db.set("day_start", d)
        return d["equity"]

    def halted(self):
        return self.db.get("halted", None)

    def paused_until(self):
        return self.db.get("paused_until", 0)

    # -- market data
    def fetch(self, tf):
        """Bitunix renvoie 200 bougies max : 1re fois on remonte 2 pages (EMA200 fiable), ensuite on fusionne."""
        conv = lambda raw: [{"t": int(k["time"]), "o": float(k["open"]), "h": float(k["high"]),
                             "l": float(k["low"]), "c": float(k["close"]), "v": float(k["baseVol"])} for k in raw or []]
        cache = {x["t"]: x for x in self.cache.get(tf, [])}
        new = conv(self.api.klines(SYMBOL, tf, limit=200))
        if not cache and new:
            oldest = min(x["t"] for x in new)
            try:
                new += conv(self.api.klines(SYMBOL, tf, limit=200, end=oldest))
            except Exception as e:
                log.warning("klines page 2 %s: %s", tf, e)
        for x in new:
            cache[x["t"]] = x
        c = sorted(cache.values(), key=lambda x: x["t"])[-450:]
        self.cache[tf] = c
        now_ms = time.time() * 1000
        if c and c[-1]["t"] + TF_SEC[tf] * 1000 > now_ms:
            live = c[-1]; c = c[:-1]
        else:
            live = None
        self.candles[tf] = c
        if tf == "1m" and live:
            self.price = live["c"]
            self.live_1m = live
        elif tf == "1m" and c:
            self.price = c[-1]["c"]
        return c

    def refresh(self):
        t = time.time()
        new_closed = []
        for tf in TIMEFRAMES:
            if t - self.last_fetch.get(tf, 0) < TF_REFRESH.get(tf, 60):
                continue
            try:
                c = self.fetch(tf)
            except Exception as e:
                log.warning("klines %s: %s", tf, e); continue
            self.last_fetch[tf] = t
            if len(c) < 30:
                continue
            self.ctx[tf] = analyse(c, tf)
            if self.last_closed.get(tf) != c[-1]["t"]:
                first = tf not in self.last_closed
                self.last_closed[tf] = c[-1]["t"]
                if not first:
                    new_closed.append(tf)
        if t - self.last_funding_fetch > 300:
            try:
                f = self.api.funding(SYMBOL)
                self.funding = float(f.get("fundingRate", 0))  # en %
                self.mark = float(f.get("markPrice", 0) or 0)
                self.last_funding_fetch = t
                self.check_funding()
            except Exception as e:
                log.warning("funding: %s", e)
        return new_closed

    def check_funding(self):
        if self.funding is None:
            return
        if abs(self.funding) >= E("FUNDING_ALERT_PCT", 0.05, float):
            side = "SHORT" if self.funding > 0 else "LONG"
            self.alert.send({"type": "setup", "timeframe": "8h", "setup": "funding_extreme", "side": side,
                             "price": self.price, "sl": None, "tp": None, "confidence": 0.5,
                             "why": f"funding {self.funding:+.3f}% (foule {'longue' if self.funding > 0 else 'courte'})",
                             "equity": round(self.equity(), 2), "open_positions": len(self.open_trades()),
                             "action": "info"}, key=f"funding:{side}")

    # -- risk
    def can_open(self, side, tf):
        h = self.halted()
        if h:
            return f"HALT: {h}"
        if time.time() < self.paused_until():
            return "pause après pertes consécutives"
        ot = self.open_trades()
        if len(ot) >= MAX_OPEN:
            return f"max {MAX_OPEN} positions"
        for t in ot:
            if t["timeframe"] == tf:
                return f"déjà une position {tf}"
            if t["side"] != side:
                return "position opposée ouverte (pas de hedge)"
            if self.price and (self.price - t["entry"]) * (1 if t["side"] == "LONG" else -1) < 0:
                return "position existante en perte (pas de moyenne à la baisse)"
        eq, d0 = self.realized_equity(), self.day_start_equity()
        if d0 and (d0 - eq) / d0 * 100 >= DAILY_STOP_PCT:
            return f"stop journalier -{DAILY_STOP_PCT}%"
        if tf not in TRADE_TIMEFRAMES:
            return f"{tf} non tradé"
        return None

    def plan(self, tf, side, a):
        px = self.price or a["close"]
        if REAL_ORDERS:  # prix frais (dernière bougie 1m en cours)
            try:
                k = self.api.klines(SYMBOL, "1m", 1)
                if k:
                    px = float(k[0]["close"]); self.price = px
            except Exception as e:
                log.warning("prix frais: %s", e)
        sl_dist = max(ATR_SL_MULT * a["atr"], px * MIN_SL_PCT / 100)
        sgn = 1 if side == "LONG" else -1
        sl, tp = px - sgn * sl_dist, px + sgn * RR * sl_dist
        eq = self.realized_equity()
        risk = eq * RISK_PCT / 100
        qty = risk / sl_dist
        max_qty = eq * MAX_MARGIN_PCT / 100 * LEVERAGE / px
        avail = None
        if REAL_ORDERS:  # clamp sur la marge DISPONIBLE (évite 20003 Insufficient balance)
            try:
                ac = self.api.account()
                ac = ac[0] if isinstance(ac, list) else ac
                avail = float(ac.get("available", 0) or 0)
                # 0.95 : marge de sécurité pour frais d'ouverture
                max_qty = min(max_qty, avail * 0.95 * LEVERAGE / px / (1 + FEE_RATE * LEVERAGE))
            except Exception as e:
                log.warning("account() pour sizing: %s", e)
        qty = round_down(max(min(qty, max_qty), 0), self.qty_prec)
        fees = FEE_RATE * qty * px * 2
        real_risk = qty * sl_dist + fees
        return {"price": round(px, 1), "sl": round(sl, self.px_prec), "tp": round(tp, self.px_prec), "qty": qty,
                "sl_dist": sl_dist, "risk_usdt": round(real_risk, 3), "fees": round(fees, 3),
                "margin": round(qty * px / LEVERAGE, 3), "fee_share": fees / max(qty * sl_dist, 1e-9),
                "available": avail}

    # -- signals
    def on_close(self, tf):
        a = self.ctx.get(tf)
        if not a:
            return
        for s in detect(tf, a, self.ctx):
            p = self.plan(tf, s["side"], a)
            sid = self.db.x("INSERT INTO signals(ts,timeframe,setup,side,price,sl,tp,confidence,why) "
                            "VALUES(?,?,?,?,?,?,?,?,?)", (now_iso(), tf, s["setup"], s["side"], p["price"],
                                                         p["sl"], p["tp"], s["confidence"], s["why"])).lastrowid
            log.info("signal %s %s %s conf=%.2f %s", tf, s["setup"], s["side"], s["confidence"], s["why"])
            skip = None
            if s.get("info_only"):
                skip = "info"
            elif s["confidence"] < MIN_CONF_TRADE:
                skip = f"confiance {s['confidence']} < {MIN_CONF_TRADE}"
            elif (s["side"] == "LONG" and not (p["sl"] < p["price"] < p["tp"])) or \
                 (s["side"] == "SHORT" and not (p["tp"] < p["price"] < p["sl"])):
                skip = f"plan invalide (prix {p['price']} sl {p['sl']} tp {p['tp']})"
                log.error("plan invalide rejeté %s %s %s", tf, s["side"], skip)
            elif p["qty"] < self.min_qty:
                skip = (f"marge disponible insuffisante ({p['available']:.2f} USDT) : qty {p['qty']} < min {self.min_qty}"
                        if p.get("available") is not None else f"qty {p['qty']} < min {self.min_qty}")
            elif p["fee_share"] > MAX_FEE_SHARE:
                skip = f"frais {p['fee_share']:.0%} du risque > {MAX_FEE_SHARE:.0%}"
            else:
                skip = self.can_open(s["side"], tf)
            acted = False
            if skip is None and AUTO_TRADE:
                acted = self.open_trade(tf, s, p, sid)
            elif skip is None:
                skip = "AUTO_TRADE=0 (décision agents)"
            self.db.x("UPDATE signals SET acted=?, skip_reason=? WHERE id=?", (1 if acted else 0, skip, sid))
            if not acted and s["confidence"] >= MIN_CONF_ALERT and not s.get("info_only") or \
               (s.get("info_only") and tf in ("15m", "1h", "4h", "1d")):
                self.alert.send({"type": "setup", "timeframe": tf, "setup": s["setup"], "side": s["side"],
                                 "price": p["price"], "sl": p["sl"], "tp": p["tp"], "confidence": s["confidence"],
                                 "why": s["why"], "qty": p["qty"], "risk_usdt": p["risk_usdt"],
                                 "equity": round(self.equity(), 2), "open_positions": len(self.open_trades()),
                                 "action": "not_taken", "skip_reason": skip, "signal_id": sid,
                                 "ctx": self.ctx_summary()}, key=f"{tf}:{s['setup']}:{s['side']}")

    def ctx_summary(self):
        out = {tf: {"trend": a["trend"], "rsi": round(a["rsi"], 1) if a["rsi"] else None,
                    "atr": round(a["atr"], 1) if a["atr"] else None} for tf, a in self.ctx.items()}
        out["funding_pct"] = self.funding
        return out

    # -- execution
    def open_trade(self, tf, s, p, sid):
        side = s["side"]
        cid = f"v4{int(time.time())}{secrets.token_hex(3)}"
        mode = self.mode
        pos_id = None
        entry = p["price"]
        if REAL_ORDERS:
            before = {}
            try:
                for ps in self.api.positions(SYMBOL):
                    before[str(ps.get("positionId"))] = (float(ps.get("qty") or 0), float(ps.get("avgOpenPrice") or 0))
            except Exception as e:
                log.warning("positions() avant ordre: %s", e)
            body = {"symbol": SYMBOL, "side": "BUY" if side == "LONG" else "SELL", "tradeSide": "OPEN",
                    "orderType": "MARKET", "qty": fmt(p["qty"], self.qty_prec), "clientId": cid,
                    "slPrice": fmt(p["sl"], self.px_prec), "slStopType": "MARK_PRICE", "slOrderType": "MARKET",
                    "tpPrice": fmt(p["tp"], self.px_prec), "tpStopType": "MARK_PRICE", "tpOrderType": "MARKET"}
            self.db.ev("order_send", body)
            try:
                self.api.place_order(body)
            except BitunixError as e:
                self.db.ev("order_rejected", {"err": str(e)}, level="ERROR")
                return False
            except Exception as e:
                self.db.ev("order_unknown", {"err": str(e), "clientId": cid}, level="ERROR")
                # on ne renvoie jamais : vérification via clientId
                try:
                    self.api.order_detail(cid)
                except Exception:
                    return False
            time.sleep(1.5)
            try:
                for ps in self.api.positions(SYMBOL):
                    if (ps.get("side") or "").upper() in (("BUY", "LONG") if side == "LONG" else ("SELL", "SHORT")):
                        pos_id = ps.get("positionId")
                        q1, a1 = float(ps.get("qty") or 0), float(ps.get("avgOpenPrice") or 0)
                        q0, a0 = before.get(str(pos_id), (0.0, 0.0))
                        # position fusionnée (hedge) : prix de remplissage = delta, pas la moyenne globale
                        if q1 - q0 > 1e-12 and a1:
                            fill = (q1 * a1 - q0 * a0) / (q1 - q0)
                            if abs(fill - entry) / entry < 0.02:
                                entry = round(fill, 1)
            except Exception as e:
                log.warning("positions(): %s", e)
        tid = self.db.x("INSERT INTO trades(mode,opened_ts,timeframe,setup,side,entry,qty,leverage,margin,sl,tp,"
                        "risk_usdt,status,client_id,position_id,signal_id,confidence,max_hold_h) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,?,?,?)",
                        (mode, now_iso(), tf, s["setup"], side, entry, p["qty"], LEVERAGE, p["margin"], p["sl"],
                         p["tp"], p["risk_usdt"], cid, pos_id, sid, s["confidence"],
                         TF_MAX_HOLD_H.get(tf, 24))).lastrowid
        self.alert.send({"type": "trade_open", "timeframe": tf, "setup": s["setup"], "side": side,
                         "price": entry, "sl": p["sl"], "tp": p["tp"], "confidence": s["confidence"],
                         "qty": p["qty"], "margin": p["margin"], "risk_usdt": p["risk_usdt"], "why": s["why"],
                         "trade_id": tid, "equity": round(self.equity(), 2),
                         "open_positions": len(self.open_trades())}, key=f"open:{tid}")
        return True

    @staticmethod
    def own_tpsl(t, orders):
        """Ordres TP/SL Bitunix appartenant à CE trade (même position, prix SL/TP du trade)."""
        out = []
        for o in orders or []:
            if str(o.get("positionId")) != str(t["position_id"]):
                continue
            sl, tp = o.get("slPrice"), o.get("tpPrice")
            if (sl and t["sl"] and abs(float(sl) - t["sl"]) < 0.06) or (tp and t["tp"] and abs(float(tp) - t["tp"]) < 0.06):
                out.append(o)
        return out

    def close_live(self, t):
        """Ferme uniquement la part de CE trade si la position exchange est partagée.
        Qty plafonnée à la taille actuelle de la position ; annule ensuite les TP/SL propres du trade."""
        others = [o for o in self.open_trades() if o["id"] != t["id"] and o["position_id"] == t["position_id"]]
        if not others:
            return self.api.flash_close(t["position_id"])
        pos_qty = None
        for ps in self.api.positions(SYMBOL):
            if str(ps.get("positionId")) == str(t["position_id"]):
                pos_qty = float(ps.get("qty") or 0)
        if not pos_qty:
            raise RuntimeError("position introuvable pour fermeture partielle")
        orders = self.api.tpsl_pending(SYMBOL)
        mine = self.own_tpsl(t, orders)
        if not mine:
            raise RuntimeError("TP/SL propres absents (trade déjà fermé ?) : pas de fermeture partielle")
        qty = round_down(min(t["qty"], pos_qty), self.qty_prec)
        if qty < self.min_qty:
            raise RuntimeError(f"qty de fermeture {qty} < min")
        r = self.api.close_qty(t["position_id"], t["side"], qty, self.qty_prec)
        for o in mine:
            try: self.api.tpsl_cancel(o.get("id"))
            except Exception as e: log.warning("tpsl_cancel %s: %s", o.get("id"), e)
        return r

    def manage(self):
        """Suivi SL/TP/temps. Paper : simulé sur la bougie 1m. Live : sync avec l'exchange."""
        if not self.price:
            return
        bars = list(self.candles.get("1m", [])[-3:])
        if getattr(self, "live_1m", None):
            bars.append(self.live_1m)
        live_pos = None
        tpsl = None
        if REAL_ORDERS and self.open_trades():
            try:
                live_pos = {str(p.get("positionId")) for p in self.api.positions(SYMBOL)}
            except Exception as e:
                log.warning("positions(): %s", e)
            try:
                tpsl = self.api.tpsl_pending(SYMBOL)
            except Exception as e:
                log.warning("tpsl_pending(): %s", e)
        for t in self.open_trades():
            opened_ms = datetime.fromisoformat(t["opened_ts"]).timestamp() * 1000
            seg = [b for b in bars if b["t"] + 60000 > opened_ms] or [{"h": self.price, "l": self.price}]
            hi, lo = max(b["h"] for b in seg), min(b["l"] for b in seg)
            exit_px = reason = None
            if REAL_ORDERS:
                if live_pos is not None and t["position_id"] and str(t["position_id"]) not in live_pos:
                    exit_px, reason = self.price, "fermée sur l'exchange (SL/TP)"
                elif tpsl is not None and live_pos is not None and t["position_id"] and \
                        time.time() * 1000 - opened_ms > 90000 and not self.own_tpsl(t, tpsl):
                    # ses propres TP/SL ont disparu -> exécutés (position fusionnée toujours ouverte)
                    hit_sl = abs(self.price - t["sl"]) <= abs(self.price - t["tp"])
                    exit_px, reason = (t["sl"], "SL propre exécuté") if hit_sl else (t["tp"], "TP propre exécuté")
            else:
                if t["side"] == "LONG":
                    if lo <= t["sl"]: exit_px, reason = t["sl"], "SL"
                    elif hi >= t["tp"]: exit_px, reason = t["tp"], "TP"
                else:
                    if hi >= t["sl"]: exit_px, reason = t["sl"], "SL"
                    elif lo <= t["tp"]: exit_px, reason = t["tp"], "TP"
            age_h = (time.time() * 1000 - opened_ms) / 3.6e6
            if exit_px is None and age_h >= (t["max_hold_h"] or 24):
                exit_px, reason = self.price, "temps max"
                if REAL_ORDERS and t["position_id"]:
                    try: self.close_live(t)
                    except Exception as e: log.error("close: %s", e); continue
            if exit_px is not None:
                self.close_trade(t, exit_px, reason)

    def close_trade(self, t, px, reason):
        sgn = 1 if t["side"] == "LONG" else -1
        fees = FEE_RATE * t["qty"] * (t["entry"] + px)
        pnl = sgn * (px - t["entry"]) * t["qty"] - fees
        self.db.x("UPDATE trades SET status='CLOSED', closed_ts=?, exit=?, pnl=?, fees=?, close_reason=? WHERE id=?",
                  (now_iso(), px, round(pnl, 4), round(fees, 4), reason, t["id"]))
        if not REAL_ORDERS:
            self.db.set("paper_equity", round(self.db.get("paper_equity", START_EQUITY) + pnl, 4))
        eq = self.equity()
        # pertes consécutives
        last = self.db.q("SELECT pnl FROM trades WHERE status='CLOSED' AND mode=? ORDER BY id DESC LIMIT ?",
                         (self.mode, MAX_CONSEC_LOSSES))
        if len(last) == MAX_CONSEC_LOSSES and all(x["pnl"] < 0 for x in last):
            self.db.set("paused_until", time.time() + PAUSE_AFTER_LOSSES_MIN * 60)
            self.db.ev("pause", {"min": PAUSE_AFTER_LOSSES_MIN}, level="WARNING")
        self.alert.send({"type": "trade_close", "timeframe": t["timeframe"], "setup": t["setup"], "side": t["side"],
                         "price": px, "entry": t["entry"], "sl": t["sl"], "tp": t["tp"], "confidence": t["confidence"],
                         "pnl": round(pnl, 4), "reason": reason, "trade_id": t["id"], "equity": round(eq, 2),
                         "open_positions": len(self.open_trades())}, key=f"close:{t['id']}")
        self.check_global(eq)

    def check_global(self, eq=None):
        eq = self.equity() if eq is None else eq
        if not self.halted() and eq <= KILL_EQUITY:
            self.db.set("halted", f"kill switch equity {eq:.2f} <= {KILL_EQUITY}")
            for t in self.open_trades():
                if REAL_ORDERS and t["position_id"]:
                    try: self.api.flash_close(t["position_id"])
                    except Exception as e: log.error("flash_close: %s", e)
                self.close_trade(t, self.price, "kill switch")
            self.alert.send({"type": "kill_switch", "equity": round(eq, 2), "price": self.price,
                             "open_positions": 0, "why": "equity sous le seuil, moteur arrêté"}, force=True)
        if eq >= TARGET_EQUITY and not self.db.get("target_hit"):
            self.db.set("target_hit", True)
            self.alert.send({"type": "target", "equity": round(eq, 2), "price": self.price,
                             "open_positions": len(self.open_trades()), "why": "objectif atteint"}, force=True)
        d0 = self.day_start_equity()
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if d0 and (d0 - eq) / d0 * 100 >= DAILY_STOP_PCT and self.db.get("daily_stop_sent") != day:
            self.db.set("daily_stop_sent", day)
            self.alert.send({"type": "risk", "equity": round(eq, 2), "price": self.price,
                             "open_positions": len(self.open_trades()),
                             "why": f"stop journalier atteint (-{DAILY_STOP_PCT}%), plus d'entrées aujourd'hui"})

    def stats(self):
        rows = self.db.q("SELECT * FROM trades WHERE status='CLOSED' AND mode=?", (self.mode,))
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today = [r for r in rows if (r["closed_ts"] or "").startswith(day)]
        wins = [r for r in rows if r["pnl"] > 0]
        return {"mode": self.mode, "equity": round(self.equity(), 2), "start": START_EQUITY,
                "trades": len(rows), "wins": len(wins), "pnl_total": round(sum(r["pnl"] for r in rows), 3),
                "trades_today": len(today), "pnl_today": round(sum(r["pnl"] for r in today), 3),
                "open": [{k: t[k] for k in ("id", "timeframe", "setup", "side", "entry", "sl", "tp", "qty")}
                         for t in self.open_trades()],
                "halted": self.halted(), "paused": time.time() < self.paused_until(),
                "signals_today": self.db.q("SELECT COUNT(*) n FROM signals WHERE substr(ts,1,10)=?", (day,))[0]["n"],
                "alerts_today": self.db.q("SELECT COUNT(*) n FROM alerts WHERE sent=1 AND substr(ts,1,10)=?", (day,))[0]["n"],
                "price": self.price, "funding_pct": self.funding}

    def digest(self):
        h = datetime.now(timezone.utc)
        key = h.strftime("%Y-%m-%d") + f"-{h.hour}"
        if h.hour in DIGEST_HOURS_UTC and self.db.get("last_digest") != key:
            self.db.set("last_digest", key)
            st = self.stats()
            self.alert.send({"type": "digest", "timeframe": "all", "setup": None, "price": self.price, "sl": None,
                             "tp": None, "confidence": None, "equity": st["equity"],
                             "open_positions": len(st["open"]), "stats": st, "ctx": self.ctx_summary(),
                             "last_trades": self.db.q("SELECT id,timeframe,setup,side,entry,exit,pnl,close_reason "
                                                      "FROM trades WHERE mode=? ORDER BY id DESC LIMIT 8",
                                                      (self.mode,))}, force=True)

    # -- setup
    def init_exchange(self):
        try:
            p = self.api.pair(SYMBOL)
            self.min_qty = float(p.get("minTradeVolume", 0.0001))
            self.qty_prec, self.px_prec = int(p.get("basePrecision", 4)), int(p.get("quotePrecision", 1))
        except Exception as e:
            log.warning("trading_pairs: %s (valeurs par défaut)", e)
        if REAL_ORDERS:
            for fn, args in ((self.api.change_margin_mode, (SYMBOL, "ISOLATION")),
                             (self.api.change_leverage, (SYMBOL, LEVERAGE))):
                try: fn(*args)
                except BitunixError as e: log.warning("%s: %s", fn.__name__, e)

    def step(self):
        for tf in self.refresh():
            self.on_close(tf)
        self.manage()
        self.check_global()
        self.digest()
        self.db.set("heartbeat", {"ts": now_iso(), "price": self.price, "equity": round(self.equity(), 3)})

    def run(self, once=False):
        self.init_exchange()
        self.db.ev("start", {"mode": self.mode, "auto_trade": AUTO_TRADE, "tfs": TIMEFRAMES, "lev": LEVERAGE,
                             "risk_pct": RISK_PCT, "webhook": bool(WEBHOOK_URL)})
        if CONTROL_PORT and not once:
            start_control(self)
        if not once and E("STARTUP_ALERT", False, bool):
            self.alert.send({"type": "startup", "equity": round(self.equity(), 2), "price": None,
                             "open_positions": len(self.open_trades()),
                             "why": f"moteur démarré ({self.mode}, x{LEVERAGE}, risque {RISK_PCT}%/trade)"}, force=True)
        while self.running:
            t0 = time.time()
            try:
                self.step()
            except Exception as e:
                log.exception("step: %s", e)
            if once:
                break
            time.sleep(max(1, POLL_SEC - (time.time() - t0)))
        self.db.ev("stop", {})

# ---------------------------------------------------------------- control API (optionnelle)
def start_control(eng):
    class H(BaseHTTPRequestHandler):
        def _auth(self):
            return CONTROL_TOKEN and self.headers.get("Authorization") == f"Bearer {CONTROL_TOKEN}"
        def _send(self, code, obj):
            b = json.dumps(obj, default=str).encode()
            self.send_response(code); self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
        def do_GET(self):
            if not self._auth(): return self._send(401, {"error": "unauthorized"})
            if self.path.startswith("/status"): return self._send(200, eng.stats())
            if self.path.startswith("/ctx"): return self._send(200, eng.ctx_summary())
            self._send(404, {"error": "not found"})
        def do_POST(self):
            if not self._auth(): return self._send(401, {"error": "unauthorized"})
            if self.path == "/pause":
                eng.db.set("halted", "pause manuelle (agents/Shaï)"); return self._send(200, {"ok": True, "halted": True})
            if self.path == "/resume":
                eng.db.set("halted", None); eng.db.set("paused_until", 0); return self._send(200, {"ok": True})
            if self.path == "/close_all":
                for t in eng.open_trades():
                    if REAL_ORDERS and t["position_id"]:
                        try: eng.api.flash_close(t["position_id"])
                        except Exception as e: return self._send(500, {"error": str(e)})
                    eng.close_trade(t, eng.price, "fermeture manuelle")
                return self._send(200, {"ok": True})
            self._send(404, {"error": "not found"})
        def log_message(self, *a): pass
    if not CONTROL_TOKEN:
        log.warning("CONTROL_PORT défini sans CONTROL_TOKEN -> API de contrôle désactivée"); return
    srv = HTTPServer((CONTROL_BIND, CONTROL_PORT), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("API de contrôle sur %s:%s", CONTROL_BIND, CONTROL_PORT)

# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--test-webhook", action="store_true")
    ap.add_argument("--status", action="store_true")
    a = ap.parse_args()
    if REAL_ORDERS:
        log.warning("*** MODE LIVE : ORDRES RÉELS ***")
    else:
        log.info("mode PAPER (DRY_RUN) — aucun ordre réel")
    eng = Engine()
    if a.test_webhook:
        ok = eng.alert.send({"type": "test", "timeframe": "15m", "setup": "test", "side": "LONG",
                             "price": 0, "sl": 0, "tp": 0, "confidence": 0, "equity": round(eng.equity(), 2),
                             "open_positions": 0, "why": "test du webhook depuis le serveur"}, force=True)
        print("webhook OK" if ok else "webhook ÉCHEC (voir logs)"); sys.exit(0 if ok else 1)
    if a.status:
        print(json.dumps(eng.stats(), indent=2, default=str, ensure_ascii=False)); return
    def stop(*_): eng.running = False
    signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
    eng.run(once=a.once)

if __name__ == "__main__":
    main()
