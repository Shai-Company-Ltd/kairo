#!/usr/bin/env python3
"""Moteur trading v4.2.2 — BTCUSDT Bitunix USDT-M, isolé, hedge, multi-timeframes.

v4.2.2 : compte HEDGE (positionId partagé) -> chaque trade possède SES ordres SL/TP (orderId en base),
dimensionnés à SA qty restante ; re-pose plafonnée à la qty libre, jamais de doublon ; 30004/taille 0 = clos.
v4.2 : TP 1.5R + clôture partielle 50 % à 1R (limite) puis SL au break-even + frais,
frais estimés maker/taker (MAX_FEE_SHARE 0.20), RISK_PCT 12, 2 positions par TF,
breakout 5m strict (vol x2), filtre ATR% par TF, confirmation VWAP/OBV, filtre Kronos 1h
(auto-activé si fiable), auto-kill setup x TF -> paper (shadow) puis réactivation.
Chaque nouveauté est désactivable dans .env (voir bloc v4.2).

v4.1 : webhooks groupés (récap 3 h), urgences immédiates (regroupées à la minute),
digest texte 1x/jour, 1m hors trading, filtre ADX, entrée limite post-only 20 s, TP limite.
Chaque nouveauté est désactivable dans .env (voir bloc v4.1).

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
RR = E("RR", 1.5, float)                          # v4.2 : TP à 1.5R (était 2.0)
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

# ---------- v4.1 (chaque nouveauté désactivable)
WEBHOOK_BATCH = E("WEBHOOK_BATCH", True, bool)          # trade_open/close/setup en tampon -> récap groupé
BATCH_HOURS = E("BATCH_HOURS", 3.0, float)              # période du récap
BATCH_SEND_EMPTY = E("BATCH_SEND_EMPTY", False, bool)   # envoyer le récap même sans événement
URGENT_COALESCE = E("URGENT_COALESCE", True, bool)      # urgences de la même minute -> un seul envoi
URGENT_DD_PCT = E("URGENT_DD_PCT", 15.0, float)         # drawdown du jour -> alerte urgente (0 = off)
URGENT_ERR_COOLDOWN_MIN = E("URGENT_ERR_COOLDOWN_MIN", 30, int)  # même erreur pas renvoyée avant N min
DIGEST_TEXT = E("DIGEST_TEXT", True, bool)              # digest = texte formaté en code
ADX_FILTER = E("ADX_FILTER", True, bool)
ADX_PERIOD = E("ADX_PERIOD", 14, int)
ADX_MIN = E("ADX_MIN", 20.0, float)
ENTRY_POST_ONLY = E("ENTRY_POST_ONLY", True, bool)      # entrée limite post-only au meilleur prix
POST_ONLY_SUPPORTED = E("POST_ONLY_SUPPORTED", True, bool)  # 0 -> limite GTC simple au meilleur prix
ENTRY_TIMEOUT_SEC = E("ENTRY_TIMEOUT_SEC", 20, int)     # non rempli -> annulé, jamais chassé
TP_LIMIT = E("TP_LIMIT", True, bool)                    # TP en ordre limite (SL toujours au marché)
# ---------- v4.2 (chaque nouveauté désactivable)
MAKER_FEE_RATE = E("MAKER_FEE_RATE", 0.0002, float)    # frais maker (entrée post-only)
FEE_MODEL_MAKER = E("FEE_MODEL_MAKER", True, bool)     # 1 = entrée maker si post-only, sortie taker (pire cas SL)
PARTIAL_TP = E("PARTIAL_TP", True, bool)               # clôture partielle à PARTIAL_R
PARTIAL_R = E("PARTIAL_R", 1.0, float)
PARTIAL_FRAC = E("PARTIAL_FRAC", 0.5, float)
PARTIAL_LIMIT = E("PARTIAL_LIMIT", True, bool)         # partielle = ordre limite de clôture posé à l'ouverture
BE_AFTER_PARTIAL = E("BE_AFTER_PARTIAL", True, bool)   # SL -> break-even + frais après la partielle
BE_FEE_PCT = E("BE_FEE_PCT", 0.10, float)              # marge BE en % du prix (couvre frais A/R ~0.08 %)
MAX_PER_TF = E("MAX_PER_TF", 1, int)                   # positions simultanées max par timeframe
BREAKOUT_VOL_MULT = E("BREAKOUT_VOL_MULT", 1.5, float)
BREAKOUT5M_STRICT = E("BREAKOUT5M_STRICT", True, bool) # 5m : vol >= BREAKOUT5M_VOL_MULT x moy20 + clôture au-delà
BREAKOUT5M_VOL_MULT = E("BREAKOUT5M_VOL_MULT", 2.0, float)
ATR_FILTER = E("ATR_FILTER", True, bool)               # marché plat (ATR% < seuil du TF) -> pas de trade
ATR_MIN_PCT = {k.strip(): float(v) for k, v in (x.split(":") for x in
               E("ATR_MIN_PCT", "5m:0.090,15m:0.165,1h:0.37,4h:0.90,1d:2.6").split(",") if ":" in x)}
VWAP_OBV = E("VWAP_OBV", "boost").strip().lower()      # off | filter | boost (bonus/malus de confiance)
VWAP_OBV_BONUS = E("VWAP_OBV_BONUS", 0.03, float)
OBV_SLOPE_BARS = E("OBV_SLOPE_BARS", 10, int)
KRONOS_FILTER = E("KRONOS_FILTER", True, bool)         # 1h : actif seulement si précision >= KRONOS_MIN_ACC
KRONOS_DB = E("KRONOS_DB", "/opt/kronos/predictions.db")
KRONOS_MIN_ACC = E("KRONOS_MIN_ACC", 0.55, float)
KRONOS_MIN_N = E("KRONOS_MIN_N", 50, int)
KRONOS_MAX_AGE_H = E("KRONOS_MAX_AGE_H", 2.0, float)
AUTO_KILL = E("AUTO_KILL", True, bool)                 # setup x TF perdant -> paper (shadow) puis réactivation
AUTO_KILL_DAYS = E("AUTO_KILL_DAYS", 3.0, float)
AUTO_KILL_MIN_N = E("AUTO_KILL_MIN_N", 8, int)
AUTO_KILL_MAX_WR = E("AUTO_KILL_MAX_WR", 0.40, float)
URGENT_TYPES = {"kill_switch", "risk", "api_error", "order_error", "no_sl", "drawdown"}

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
        d = self.request("GET", "/api/v1/futures/tpsl/get_pending_orders", {"symbol": symbol, "limit": 100}, private=True)
        return d if isinstance(d, list) else (d or {}).get("orderList", []) if isinstance(d, dict) else []
    def tpsl_history(self, symbol=None):
        d = self.request("GET", "/api/v1/futures/tpsl/get_history_orders", {"symbol": symbol, "limit": 100}, private=True)
        return d if isinstance(d, list) else (d or {}).get("orderList", []) if isinstance(d, dict) else []
    def tpsl_place(self, position_id, qty, sl, tp, qprec=4, pprec=1):
        body = {"symbol": SYMBOL, "positionId": str(position_id),
                "slPrice": fmt(sl, pprec), "slStopType": "MARK_PRICE", "slOrderType": "MARKET", "slQty": fmt(qty, qprec),
                "tpPrice": fmt(tp, pprec), "tpStopType": "MARK_PRICE", "tpOrderType": "MARKET", "tpQty": fmt(qty, qprec)}
        if TP_LIMIT:   # v4.1 : TP limite au prix du TP ; SL reste au marché
            body["tpOrderType"], body["tpOrderPrice"] = "LIMIT", fmt(tp, pprec)
        return self.request("POST", "/api/v1/futures/tpsl/place_order", body=body, private=True, retries=1)
    def tpsl_modify_sl(self, order_id, sl, qty, qprec=4, pprec=1):
        """v4.2.1 : modification EN PLACE (même orderId) -> la position n'est jamais sans SL."""
        return self.request("POST", "/api/v1/futures/tpsl/modify_order", private=True, retries=1, body={
            "orderId": str(order_id), "slPrice": fmt(sl, pprec), "slStopType": "MARK_PRICE",
            "slOrderType": "MARKET", "slQty": fmt(qty, qprec)})
    def tpsl_modify_tp(self, order_id, tp, qty, qprec=4, pprec=1):
        body = {"orderId": str(order_id), "tpPrice": fmt(tp, pprec), "tpStopType": "MARK_PRICE",
                "tpOrderType": "MARKET", "tpQty": fmt(qty, qprec)}
        if TP_LIMIT:
            body["tpOrderType"], body["tpOrderPrice"] = "LIMIT", fmt(tp, pprec)
        return self.request("POST", "/api/v1/futures/tpsl/modify_order", body=body, private=True, retries=1)
    def tpsl_place_one(self, position_id, qty, sl=None, tp=None, qprec=4, pprec=1):
        body = {"symbol": SYMBOL, "positionId": str(position_id)}
        if sl is not None:
            body.update({"slPrice": fmt(sl, pprec), "slStopType": "MARK_PRICE", "slOrderType": "MARKET", "slQty": fmt(qty, qprec)})
        if tp is not None:
            body.update({"tpPrice": fmt(tp, pprec), "tpStopType": "MARK_PRICE", "tpOrderType": "MARKET", "tpQty": fmt(qty, qprec)})
            if TP_LIMIT:
                body["tpOrderType"], body["tpOrderPrice"] = "LIMIT", fmt(tp, pprec)
        return self.request("POST", "/api/v1/futures/tpsl/place_order", body=body, private=True, retries=1)
    def tpsl_cancel(self, order_id):
        return self.request("POST", "/api/v1/futures/tpsl/cancel_order",
                            body={"symbol": SYMBOL, "orderId": str(order_id)}, private=True, retries=1)
    def depth(self, symbol, limit=1):
        return self.request("GET", "/api/v1/futures/market/depth", {"symbol": symbol, "limit": limit})
    def cancel_orders(self, symbol, order_ids=(), client_ids=()):
        ol = [{"orderId": str(i)} for i in order_ids] + [{"clientId": str(c)} for c in client_ids]
        return self.request("POST", "/api/v1/futures/trade/cancel_orders",
                            body={"symbol": symbol, "orderList": ol}, private=True, retries=1)
    def pending_orders(self, symbol=None):
        d = self.request("GET", "/api/v1/futures/trade/get_pending_orders", {"symbol": symbol}, private=True)
        return d if isinstance(d, list) else (d or {}).get("orderList", []) if isinstance(d, dict) else []
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

def adx(c, n=14):
    """ADX de Wilder. None si historique insuffisant."""
    if len(c) < 2 * n + 2:
        return None
    trs, pdm, mdm = [], [], []
    for i in range(1, len(c)):
        up, dn = c[i]["h"] - c[i - 1]["h"], c[i - 1]["l"] - c[i]["l"]
        pdm.append(up if up > dn and up > 0 else 0.0)
        mdm.append(dn if dn > up and dn > 0 else 0.0)
        trs.append(max(c[i]["h"] - c[i]["l"], abs(c[i]["h"] - c[i - 1]["c"]), abs(c[i]["l"] - c[i - 1]["c"])))
    tr_s, p_s, m_s = sum(trs[:n]), sum(pdm[:n]), sum(mdm[:n])
    dxs = []
    for i in range(n, len(trs) + 1):
        if i > n:
            tr_s = tr_s - tr_s / n + trs[i - 1]; p_s = p_s - p_s / n + pdm[i - 1]; m_s = m_s - m_s / n + mdm[i - 1]
        if tr_s <= 0:
            dxs.append(0.0); continue
        pdi, mdi = 100 * p_s / tr_s, 100 * m_s / tr_s
        dxs.append(0.0 if pdi + mdi == 0 else 100 * abs(pdi - mdi) / (pdi + mdi))
    if len(dxs) < n:
        return None
    a = sum(dxs[:n]) / n
    for d in dxs[n:]:
        a = (a * (n - 1) + d) / n
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

def obv_slope(c, n=10):
    """Pente OBV : OBV[-1] - OBV[-1-n], normalisée par le volume moyen (>0 = acheteurs)."""
    if len(c) < n + 2:
        return None
    obv, o = [0.0], 0.0
    for i in range(1, len(c)):
        d = c[i]["c"] - c[i - 1]["c"]
        o += c[i]["v"] if d > 0 else -c[i]["v"] if d < 0 else 0.0
        obv.append(o)
    vavg = sum(x["v"] for x in c[-n:]) / n or 1.0
    return (obv[-1] - obv[-1 - n]) / vavg

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
            "rsi": rsi(cl), "rsi_prev": rsi(cl[:-1]), "atr": atr(c), "adx": adx(c, ADX_PERIOD),
            "vwap": session_vwap(c) if TF_SEC.get(tf, 0) <= 3600 else None,
            "vwap50": vwap(c, 50), "vol": last["v"], "vol_avg": vavg,
            "vol_ratio": (last["v"] / vavg) if vavg else None,
            "hi20": hi20, "lo20": lo20, "trend": trend,
            "atr_pct": (atr(c) or 0) / last["c"] * 100 if last["c"] else None,
            "obv_slope": obv_slope(c, OBV_SLOPE_BARS)}

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

    # v4.2 : les setups filtrés (ADX, ATR%, volume breakout 5m) sont RETOURNÉS avec "filtered"
    # -> journalisés dans signals.skip_reason (comptables), jamais tradés ni alertés.
    adx_skip = None
    if ADX_FILTER and not (a.get("adx") is not None and a["adx"] > ADX_MIN):
        adx_skip = f"adx {a['adx'] if a.get('adx') is not None else float('nan'):.1f} <= {ADX_MIN:g}"
    atr_skip = None
    thr = ATR_MIN_PCT.get(tf)
    if ATR_FILTER and thr and a.get("atr_pct") is not None and a["atr_pct"] < thr:
        atr_skip = f"atr% {a['atr_pct']:.3f} < {thr:g} ({tf} plat)"
    strict5 = BREAKOUT5M_STRICT and tf == "5m"
    bvol = BREAKOUT5M_VOL_MULT if strict5 else BREAKOUT_VOL_MULT
    def add(d, trend_gate=True):
        f = (adx_skip if trend_gate else None) or atr_skip
        if f and not d.get("filtered"):
            d["filtered"] = f
        out.append(d)
    # 1) cassure du range 20 bougies avec volume (ADX > ADX_MIN ; 5m : vol x2 + clôture au-delà, bougie dans le sens)
    if a["hi20"] and px > a["hi20"] and vr >= 1.5 and a["trend"] >= 0:
        d = {"setup": "breakout", "side": "LONG", "confidence": conf(0.55, 1),
             "why": f"close>{a['hi20']:.1f} (haut 20), vol x{vr:.1f}"}
        if vr < bvol:
            d["filtered"] = f"breakout vol x{vr:.1f} < {bvol:g}" + (" (5m)" if strict5 else "")
        elif strict5 and not px > a["open"]:
            d["filtered"] = "breakout 5m : bougie non haussière"
        add(d)
    if a["lo20"] and px < a["lo20"] and vr >= 1.5 and a["trend"] <= 0:
        d = {"setup": "breakout", "side": "SHORT", "confidence": conf(0.55, -1),
             "why": f"close<{a['lo20']:.1f} (bas 20), vol x{vr:.1f}"}
        if vr < bvol:
            d["filtered"] = f"breakout vol x{vr:.1f} < {bvol:g}" + (" (5m)" if strict5 else "")
        elif strict5 and not px < a["open"]:
            d["filtered"] = "breakout 5m : bougie non baissière"
        add(d)
    # 2) pullback sur EMA20 dans la tendance
    if a["trend"] == 1 and a["low"] <= a["ema20"] < px and 40 <= a["rsi"] <= 60 and px > a["open"]:
        add({"setup": "pullback_ema20", "side": "LONG", "confidence": conf(0.52, 1),
             "why": f"tendance haussière, rebond EMA20 {a['ema20']:.1f}, RSI {a['rsi']:.0f}"})
    if a["trend"] == -1 and a["high"] >= a["ema20"] > px and 40 <= a["rsi"] <= 60 and px < a["open"]:
        add({"setup": "pullback_ema20", "side": "SHORT", "confidence": conf(0.52, -1),
             "why": f"tendance baissière, rejet EMA20 {a['ema20']:.1f}, RSI {a['rsi']:.0f}"})
    # 3) excès RSI + spike de volume + bougie de retournement (contre-tendance, confiance plus basse)
    if a["rsi_prev"] is not None:
        if a["rsi_prev"] < 25 and a["rsi"] > a["rsi_prev"] and vr >= 2 and px > a["open"]:
            add({"setup": "rsi_reversal", "side": "LONG", "confidence": conf(0.45, 1),
                 "why": f"RSI {a['rsi_prev']:.0f}->{a['rsi']:.0f}, capitulation vol x{vr:.1f}"}, trend_gate=False)
        if a["rsi_prev"] > 75 and a["rsi"] < a["rsi_prev"] and vr >= 2 and px < a["open"]:
            add({"setup": "rsi_reversal", "side": "SHORT", "confidence": conf(0.45, -1),
                 "why": f"RSI {a['rsi_prev']:.0f}->{a['rsi']:.0f}, euphorie vol x{vr:.1f}"}, trend_gate=False)
    # 4) spike de volume pur (info, pas de trade)
    if vr >= 4:
        side = "LONG" if px > a["open"] else "SHORT"
        out.append({"setup": "volume_spike", "side": side, "confidence": 0.5, "info_only": True,
                    "why": f"volume x{vr:.1f} la moyenne, bougie {'verte' if side == 'LONG' else 'rouge'}"})
    # v4.2 : confirmation VWAP de session + pente OBV (LONG : prix > VWAP et OBV montant ; SHORT inverse)
    if VWAP_OBV in ("filter", "boost"):
        for d in out:
            if d.get("info_only") or d.get("filtered"):
                continue
            sg = 1 if d["side"] == "LONG" else -1
            ok_v = a.get("vwap") is None or (px - a["vwap"]) * sg > 0
            ok_o = a.get("obv_slope") is None or a["obv_slope"] * sg > 0
            if VWAP_OBV == "filter":
                if not (ok_v and ok_o):
                    d["filtered"] = f"vwap/obv non confirmé (vwap {'ok' if ok_v else 'non'}, obv {'ok' if ok_o else 'non'})"
            else:
                b = VWAP_OBV_BONUS if (ok_v and ok_o) else -VWAP_OBV_BONUS if not (ok_v or ok_o) else 0.0
                if b:
                    d["confidence"] = round(max(0.0, min(0.95, d["confidence"] + b)), 2)
                    d["why"] += f", vwap/obv {b:+.2f}"
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
CREATE TABLE IF NOT EXISTS shadow_trades(id INTEGER PRIMARY KEY, opened_ts TEXT, closed_ts TEXT, timeframe TEXT,
  setup TEXT, side TEXT, entry REAL, qty REAL, sl REAL, tp REAL, init_sl REAL, partial_done INTEGER DEFAULT 0,
  partial_pnl REAL DEFAULT 0, exit REAL, pnl REAL, status TEXT, close_reason TEXT, signal_id INTEGER, max_hold_h REAL);
"""
TRADE_COLS_V42 = {"init_sl": "REAL", "orig_qty": "REAL", "partial_done": "INTEGER DEFAULT 0", "partial_qty": "REAL",
                  "partial_px": "REAL", "partial_pnl": "REAL DEFAULT 0", "partial_fees": "REAL DEFAULT 0",
                  "partial_cid": "TEXT",
                  # v4.2.2 : identité des ordres TP/SL par trade (compte HEDGE, positionId partagé)
                  "sl_order_id": "TEXT", "tp_order_id": "TEXT"}

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

class DB:
    def __init__(self, path):
        self.c = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.c.execute("PRAGMA journal_mode=WAL")
        self.c.executescript(SCHEMA)
        have = {r[1] for r in self.c.execute("PRAGMA table_info(trades)")}
        for col, typ in TRADE_COLS_V42.items():   # v4.2 : migration douce (trades existants : NULL = pas de partielle)
            if col not in have:
                self.c.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
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
    PRIORITY = {"trade_open", "trade_close", "risk", "kill_switch", "target", "test", "digest", "startup",
                "batch_summary", "urgent_batch", "api_error", "order_error", "no_sl", "drawdown"}

    ALWAYS = {"trade_open", "trade_close", "kill_switch"}

    def __init__(self, db):
        self.db = db
        self.s = requests.Session()

    def _today(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ---- v4.1 : routage tampon / urgent
    BATCHED = {"trade_open", "trade_close", "setup", "target", "partial", "autokill"}

    def send(self, payload, key=None, force=False):
        typ = payload.get("type", "alert")
        if typ in URGENT_TYPES:
            item = {"ts": now_iso(), "key": key, **payload}
            if URGENT_COALESCE and typ != "kill_switch":
                buf = self.db.get("urgent_buf", []) or []
                buf.append(item); self.db.set("urgent_buf", buf)
                log.warning("urgence en file (envoi groupé à la minute): %s %s", typ, payload.get("why"))
                return True
            buf = (self.db.get("urgent_buf", []) or []) + [item]
            self.db.set("urgent_buf", [])
            return self._send_urgent(buf)
        if WEBHOOK_BATCH and typ in self.BATCHED:
            buf = self.db.get("batch_buf", []) or []
            buf.append({"ts": now_iso(), "key": key, **payload}); self.db.set("batch_buf", buf[-300:])
            self.db.x("INSERT INTO alerts(ts,type,key,payload,sent,suppressed_reason) VALUES(?,?,?,?,0,?)",
                      (now_iso(), typ, key, json.dumps(payload, default=str), "batch"))
            log.info("événement mis en tampon (récap %sh): %s %s", BATCH_HOURS, typ, key)
            return True
        return self.send_now(payload, key, force)

    @staticmethod
    def _code(lines):
        return "```\n" + "\n".join(lines) + "\n```"

    def _send_urgent(self, items):
        if not items:
            return True
        lines = ["URGENT trading-v4 %s (%s)" % (SYMBOL, "LIVE" if REAL_ORDERS else "PAPER")]
        for it in items:
            lines.append("%s %-11s %s" % (it["ts"][11:19], it.get("type"), it.get("why") or ""))
            if it.get("equity") is not None:
                lines.append("         equity %s  prix %s  pos %s" % (it.get("equity"), it.get("price"), it.get("open_positions")))
        text = self._code(lines)
        if len(items) == 1:
            p = {k: v for k, v in items[0].items() if k not in ("ts", "key")}
            p["text"] = text; p["urgent"] = True
            return self.send_now(p, items[0].get("key"), force=True)
        return self.send_now({"type": "urgent_batch", "urgent": True, "items": items, "text": text,
                              "why": "; ".join(f"{i.get('type')}: {i.get('why')}" for i in items)[:500]},
                             "urgent_batch", force=True)

    def tick(self, eng):
        """Appelé à chaque boucle : vide les urgences (minute écoulée) et le récap périodique."""
        buf = self.db.get("urgent_buf", []) or []
        if buf:
            first = buf[0]["ts"][:16]
            if not URGENT_COALESCE or now_iso()[:16] != first:
                self.db.set("urgent_buf", [])
                self._send_urgent(buf)
        if not WEBHOOK_BATCH:
            return
        bucket = int(time.time() // max(BATCH_HOURS * 3600, 60))
        last = self.db.get("batch_bucket")
        if last is None:
            self.db.set("batch_bucket", bucket); return
        if bucket == last:
            return
        self.db.set("batch_bucket", bucket)
        items = self.db.get("batch_buf", []) or []
        if not items and not BATCH_SEND_EMPTY:
            return
        self.db.set("batch_buf", [])
        self.send_now(eng.batch_payload(items), "batch", force=True)

    def send_now(self, payload, key=None, force=False):
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

def fee_estimate(qty, px):
    """v4.2 : entrée post-only = maker, sortie = taker (pire cas : SL au marché)."""
    entry_rate = MAKER_FEE_RATE if (FEE_MODEL_MAKER and ENTRY_POST_ONLY) else FEE_RATE
    return (entry_rate + FEE_RATE) * qty * px

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

    # -- v4.1 alertes urgentes (dédupliquées)
    def urgent(self, typ, why, key=None, **extra):
        k = key or f"{typ}:{str(why)[:60]}"
        last = self.db.get("urgent_last", {}) or {}
        t = time.time()
        if t - float(last.get(k, 0)) < URGENT_ERR_COOLDOWN_MIN * 60:
            log.warning("urgence déjà signalée (cooldown) %s: %s", k, why); return
        last = {kk: v for kk, v in last.items() if t - float(v) < 86400}
        last[k] = t; self.db.set("urgent_last", last)
        eq = self.db.get("live_equity") if REAL_ORDERS else self.db.get("paper_equity")
        try:
            n_open = len(self.open_trades())
        except Exception:
            n_open = None
        self.alert.send({"type": typ, "why": str(why)[:400], "price": self.price,
                         "equity": round(eq, 2) if isinstance(eq, (int, float)) else eq,
                         "open_positions": n_open, **extra}, key=k, force=True)

    def batch_payload(self, items):
        st = self.stats()
        opens = [i for i in items if i.get("type") == "trade_open"]
        closes = [i for i in items if i.get("type") == "trade_close"]
        setups = [i for i in items if i.get("type") == "setup"]
        pnl = round(sum(float(i.get("pnl") or 0) for i in closes), 4)
        L = [f"RECAP {BATCH_HOURS:g}h trading-v4 {SYMBOL} {self.mode}  {now_iso()[:16]}Z",
             f"equity {st['equity']}  pnl période {pnl:+}  pnl jour {st['pnl_today']:+}  pnl total {st['pnl_total']:+}",
             f"ouverts {len(opens)}  fermés {len(closes)}  setups {len(setups)}  positions {len(st['open'])}", ""]
        for i in items:
            ts = (i.get("ts") or "")[11:16]
            if i.get("type") == "trade_open":
                L.append(f"{ts} OPEN  #{i.get('trade_id')} {i.get('timeframe'):>3} {i.get('side'):<5} {i.get('setup')} "
                         f"@{i.get('price')} sl {i.get('sl')} tp {i.get('tp')} q {i.get('qty')}")
            elif i.get("type") == "trade_close":
                L.append(f"{ts} CLOSE #{i.get('trade_id')} {i.get('timeframe'):>3} {i.get('side'):<5} "
                         f"{i.get('entry')}->{i.get('price')} pnl {float(i.get('pnl') or 0):+.4f} ({i.get('reason')})")
            elif i.get("type") == "setup":
                L.append(f"{ts} SETUP {i.get('timeframe')} {i.get('setup')} {i.get('side')} conf {i.get('confidence')} "
                         f"({i.get('skip_reason') or i.get('action')})")
            else:
                L.append(f"{ts} {str(i.get('type')).upper()} {i.get('why') or ''}")
        if st["open"]:
            L += ["", "positions ouvertes :"] + [f"  #{o['id']} {o['timeframe']} {o['side']} @{o['entry']} sl {o['sl']} tp {o['tp']} q {o['qty']}"
                                                 for o in st["open"]]
        text = Alerter._code(L)
        return {"type": "batch_summary", "equity": st["equity"], "price": self.price, "open_positions": len(st["open"]),
                "pnl_period": pnl, "trades_opened": len(opens), "trades_closed": len(closes), "text": text,
                "why": f"récap {BATCH_HOURS:g}h : {len(opens)} ouverts, {len(closes)} fermés, pnl {pnl:+}",
                "events": items[-60:]}

    def digest_text(self, st):
        L = [f"DIGEST quotidien trading-v4 {SYMBOL} {self.mode}  {now_iso()[:16]}Z",
             f"equity {st['equity']} (départ {st['start']})  halted {st['halted'] or 'non'}  pause {st['paused']}",
             f"trades total {st['trades']}  gagnants {st['wins']}  pnl total {st['pnl_total']:+}",
             f"aujourd'hui (UTC) : {st['trades_today']} trades  pnl {st['pnl_today']:+}  signaux {st['signals_today']}",
             f"prix {st['price']}  funding {st['funding_pct']}%", "", "contexte :"]
        for tf, a in self.ctx.items():
            L.append(f"  {tf:>3} trend {a['trend']:+d}  rsi {a['rsi'] or 0:5.1f}  adx {a.get('adx') or 0:5.1f}  atr {a['atr'] or 0:.1f}")
        if st["open"]:
            L += ["", "positions :"] + [f"  #{o['id']} {o['timeframe']} {o['side']} @{o['entry']} sl {o['sl']} tp {o['tp']} q {o['qty']}"
                                        for o in st["open"]]
        rows = self.db.q("SELECT id,timeframe,setup,side,entry,exit,pnl,close_reason FROM trades WHERE mode=? "
                         "AND status='CLOSED' ORDER BY id DESC LIMIT 8", (self.mode,))
        if rows:
            L += ["", "derniers trades :"] + [f"  #{r['id']} {r['timeframe']} {r['side']} {r['entry']}->{r['exit']} "
                                              f"{(r['pnl'] or 0):+.4f} {r['close_reason']}" for r in rows]
        return Alerter._code(L)

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
        if sum(1 for t in ot if t["timeframe"] == tf) >= MAX_PER_TF:
            return f"déjà {MAX_PER_TF} position(s) {tf}"
        for t in ot:
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
        fees = fee_estimate(qty, px)
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
            if s.get("filtered") is None and not s.get("info_only") and tf == "1h":
                k = self.kronos_veto(s["side"])
                if k:
                    s["filtered"] = k
            if s.get("filtered"):   # v4.2 : filtre journalisé (comptable), pas de plan / alerte / ordre
                self.db.x("INSERT INTO signals(ts,timeframe,setup,side,price,confidence,why,acted,skip_reason) "
                          "VALUES(?,?,?,?,?,?,?,0,?)", (now_iso(), tf, s["setup"], s["side"], a["close"],
                                                         s["confidence"], s["why"], "filtre: " + s["filtered"]))
                log.info("signal filtré %s %s %s : %s", tf, s["setup"], s["side"], s["filtered"])
                continue
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
            elif self.killed(tf, s["setup"]):
                skip = "auto-kill: paper (shadow)"
                self.shadow_open(tf, s, p, sid)
            else:
                skip = self.can_open(s["side"], tf)
            acted = False
            if skip is None and AUTO_TRADE:
                acted = self.open_trade(tf, s, p, sid)
            elif skip is None:
                skip = "AUTO_TRADE=0 (décision agents)"
            self.db.x("UPDATE signals SET acted=?, skip_reason=? WHERE id=?", (1 if acted else 0, skip, sid))
            if not acted and s["confidence"] >= MIN_CONF_ALERT and not s.get("info_only") and \
               not str(skip or "").startswith("auto-kill") or \
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
            if TP_LIMIT:
                body["tpOrderType"], body["tpOrderPrice"] = "LIMIT", fmt(p["tp"], self.px_prec)
            limit_px = None
            if ENTRY_POST_ONLY:
                r = self._entry_limit(body, side, p, cid)
                if r is None:
                    return False
                limit_px, filled = r
                if filled + 1e-12 < p["qty"]:
                    p = dict(p, qty=filled, margin=round(filled * limit_px / LEVERAGE, 3))
            else:
                self.db.ev("order_send", body)
                try:
                    self.api.place_order(body)
                except BitunixError as e:
                    self.db.ev("order_rejected", {"err": str(e)}, level="ERROR")
                    self.urgent("order_error", f"ordre refusé: {e}", key=f"ord:{getattr(e, 'code', '')}")
                    return False
                except Exception as e:
                    self.db.ev("order_unknown", {"err": str(e), "clientId": cid}, level="ERROR")
                    # on ne renvoie jamais : vérification via clientId
                    try:
                        self.api.order_detail(cid)
                    except Exception:
                        self.urgent("order_error", f"ordre au statut inconnu {cid}: {e}", key=f"ordunk:{cid}")
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
            if limit_px:
                entry = round(limit_px, self.px_prec)   # post-only : rempli exactement au prix limite
        tid = self.db.x("INSERT INTO trades(mode,opened_ts,timeframe,setup,side,entry,qty,leverage,margin,sl,tp,"
                        "risk_usdt,status,client_id,position_id,signal_id,confidence,max_hold_h,init_sl,orig_qty,partial_done) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,?,?,?,?,?,0)",
                        (mode, now_iso(), tf, s["setup"], side, entry, p["qty"], LEVERAGE, p["margin"], p["sl"],
                         p["tp"], p["risk_usdt"], cid, pos_id, sid, s["confidence"],
                         TF_MAX_HOLD_H.get(tf, 24), p["sl"] if PARTIAL_TP else None, p["qty"])).lastrowid
        if REAL_ORDERS:
            self.verify_entry_tpsl(tid, partial=bool(limit_px) and p["qty"] + 1e-12 < float(body["qty"]))
            if PARTIAL_TP and PARTIAL_LIMIT:
                self.place_partial_limit(self.db.q("SELECT * FROM trades WHERE id=?", (tid,))[0])
                time.sleep(1.0)
                self.verify_entry_tpsl(tid)   # le SL doit toujours être là après la pose de la partielle
        self.alert.send({"type": "trade_open", "timeframe": tf, "setup": s["setup"], "side": side,
                         "price": entry, "sl": p["sl"], "tp": p["tp"], "confidence": s["confidence"],
                         "qty": p["qty"], "margin": p["margin"], "risk_usdt": p["risk_usdt"], "why": s["why"],
                         "trade_id": tid, "equity": round(self.equity(), 2),
                         "open_positions": len(self.open_trades())}, key=f"open:{tid}")
        return True

    def _entry_limit(self, body, side, p, cid):
        """v4.1 : entrée limite post-only au meilleur prix ; annulée (jamais chassée) après ENTRY_TIMEOUT_SEC.
        Retourne (prix, qty remplie) ou None."""
        try:
            d = self.api.depth(SYMBOL, 1)
            bid, ask = float(d["bids"][0][0]), float(d["asks"][0][0])
        except Exception as e:
            self.urgent("api_error", f"depth() pour entrée limite: {e}", key="depth"); return None
        px = round(bid if side == "LONG" else ask, self.px_prec)
        if not ((side == "LONG" and p["sl"] < px < p["tp"]) or (side == "SHORT" and p["tp"] < px < p["sl"])):
            self.db.ev("entry_skip", {"why": "prix limite hors sl/tp", "px": px, "sl": p["sl"], "tp": p["tp"]}, level="WARNING")
            return None
        body.update({"orderType": "LIMIT", "price": fmt(px, self.px_prec),
                     "effect": "POST_ONLY" if POST_ONLY_SUPPORTED else "GTC"})
        self.db.ev("order_send", body)
        try:
            self.api.place_order(body)
        except BitunixError as e:
            self.db.ev("order_rejected", {"err": str(e)}, level="ERROR")
            if "post" in str(e).lower():   # post-only qui aurait croisé : normal, pas de chasse
                self.db.ev("entry_postonly_rejected", {"err": str(e)}, level="WARNING"); return None
            self.urgent("order_error", f"ordre limite refusé: {e}", key=f"ord:{getattr(e, 'code', '')}")
            return None
        except Exception as e:
            self.db.ev("order_unknown", {"err": str(e), "clientId": cid}, level="ERROR")
        status, tq, det = None, 0.0, {}
        deadline = time.time() + ENTRY_TIMEOUT_SEC
        while time.time() < deadline:
            time.sleep(2)
            try:
                det = self.api.order_detail(cid) or {}
                status, tq = str(det.get("status") or "").upper().strip("_"), float(det.get("tradeQty") or 0)
            except Exception as e:
                log.warning("order_detail %s: %s", cid, e); continue
            if status in ("FILLED", "CANCELED", "CANCELLED"):
                break
        if status != "FILLED":
            if status not in ("CANCELED", "CANCELLED"):
                try:
                    r = self.api.cancel_orders(SYMBOL, client_ids=[cid])
                    if (r or {}).get("failureList"):
                        log.warning("cancel %s: %s", cid, r.get("failureList"))
                except Exception as e:
                    log.warning("cancel_orders %s: %s", cid, e)
                time.sleep(1.0)
            try:
                det = self.api.order_detail(cid) or {}
                status, tq = str(det.get("status") or "").upper().strip("_"), float(det.get("tradeQty") or 0)
            except Exception as e:
                self.urgent("order_error", f"ordre limite {cid} : statut illisible après annulation ({e})", key=f"ordunk:{cid}")
                return None
            if status in ("NEW", "INIT", "PART_FILLED"):
                self.urgent("order_error", f"ordre limite {cid} non annulé (statut {status})", key=f"ordunk:{cid}")
        tq = round_down(tq, self.qty_prec)
        if tq < self.min_qty:
            self.db.ev("entry_unfilled", {"clientId": cid, "px": px, "status": status, "timeout_s": ENTRY_TIMEOUT_SEC})
            return None
        if status != "FILLED":
            self.db.ev("entry_partial", {"clientId": cid, "filled": tq, "qty": body["qty"]}, level="WARNING")
        try:
            px = float(det.get("avgPrice") or det.get("price") or px)
        except (TypeError, ValueError):
            pass
        return px, tq

    def verify_entry_tpsl(self, tid, partial=False):
        """Après ouverture : le trade doit avoir SON SL (et TP) sur Bitunix ; sinon on les pose.
        v4.2.2 : les ordres posés avec l'entrée sont adoptés (orderId stocké en base) ; ensuite tout passe par l'id."""
        t = self._fresh(tid)
        if not t["position_id"]:
            self.urgent("no_sl", f"#{tid} position introuvable après ouverture : SL non vérifiable", key=f"nosl:{tid}"); return
        try:
            self.adopt_ids(t, self.pend(t["position_id"]))
        except Exception as e:
            log.warning("verify tpsl_pending: %s", e); return
        if partial:   # ramener à la qty remplie EN PLACE (même orderId)
            self.swap_tpsl(t, t["qty"], t["sl"], t["tp"])
        self.ensure_tpsl(t)

    # ---------------------------------------------------------------- v4.2.2 : identité TP/SL par orderId
    # Compte HEDGE : tous les trades d'un même sens partagent UN positionId. Chaque trade possède SES ordres
    # (sl_order_id / tp_order_id en base), dimensionnés à SA qty restante. Plus aucun rapprochement par prix
    # (sauf adoption unique des ordres posés avec l'entrée, qui ne renvoie pas d'id).
    @staticmethod
    def _kind(o):
        return "sl" if o.get("slPrice") else "tp"

    @staticmethod
    def _oqty(o):
        k = "slQty" if o.get("slPrice") else "tpQty"
        try: return float(o.get(k) or 0)
        except (TypeError, ValueError): return 0.0

    def _fresh(self, t):
        tid = t if isinstance(t, int) else t["id"]
        r = self.db.q("SELECT * FROM trades WHERE id=?", (tid,))
        return r[0] if r else None

    def _set_oid(self, t, kind, oid):
        oid = str(oid) if oid else None
        self.db.x(f"UPDATE trades SET {kind}_order_id=? WHERE id=?", (oid, t["id"]))
        t[f"{kind}_order_id"] = oid

    def _claimed(self, exclude=None):
        s = set()
        for o in self.open_trades():
            if o["id"] != exclude:
                s.update(str(o[k]) for k in ("sl_order_id", "tp_order_id") if o.get(k))
        return s

    def pend(self, pid=None):
        return [o for o in self.api.tpsl_pending(SYMBOL) if pid is None or str(o.get("positionId")) == str(pid)]

    def pos_qty(self, pid):
        return sum(float(ps.get("qty") or 0) for ps in self.api.positions(SYMBOL)
                   if str(ps.get("positionId")) == str(pid))

    def own_tpsl(self, t, orders):
        """Ordres TP/SL de CE trade : uniquement par orderId (jamais par prix)."""
        ids = {str(t[k]) for k in ("sl_order_id", "tp_order_id") if t.get(k)}
        return [o for o in orders or [] if str(o.get("id")) in ids]

    def adopt_ids(self, t, pend):
        """Trade sans orderId pour sl/tp (entrée avec TP/SL attachés, ou trade d'avant v4.2.2) :
        adopte UNE fois l'ordre en attente non revendiqué, même position, même prix (qty la plus proche)."""
        need = [k for k in ("sl", "tp") if not t.get(f"{k}_order_id")]
        if not need or not t.get("position_id"):
            return
        claimed = self._claimed(exclude=t["id"])
        for kind in need:
            c = [o for o in pend or [] if str(o.get("positionId")) == str(t["position_id"]) and self._kind(o) == kind
                 and str(o.get("id")) not in claimed and o.get(f"{kind}Price")
                 and abs(float(o[f"{kind}Price"]) - float(t[kind])) < 0.06]
            if c:
                o = min(c, key=lambda o: abs(self._oqty(o) - t["qty"]))
                self._set_oid(t, kind, o.get("id"))
                self.db.ev("tpsl_adopted", {"trade": t["id"], "kind": kind, "order": o.get("id"), "qty": self._oqty(o)})

    def _free_qty(self, t, kind, pos, pend):
        """Qty de la position non couverte par les ordres `kind` (sl ou tp) des AUTRES trades (et orphelins)."""
        mine = {str(t.get(f"{kind}_order_id"))}
        used = sum(self._oqty(o) for o in pend if self._kind(o) == kind and str(o.get("id")) not in mine)
        return max(0.0, pos - used)

    def _tp_limit_live(self, t):
        """TP limite déclenché : l'ordre limite de clôture est encore en carnet -> ne rien doubler."""
        if not TP_LIMIT:
            return False
        try:
            for o in self.api.pending_orders(SYMBOL):
                if str(o.get("positionId")) == str(t["position_id"]) and o.get("price") and \
                        abs(float(o["price"]) - float(t["tp"])) < 0.06 and \
                        str(o.get("clientId") or "") != str(t.get("partial_cid") or "-"):
                    return True
        except Exception as e:
            log.warning("pending_orders: %s", e); return True   # dans le doute : pas de doublon
        return False

    def _place_kind(self, t, kind, price, q):
        """Pose UN ordre (sl ou tp) de qty q pour le trade et enregistre son orderId."""
        r = self.api.tpsl_place_one(t["position_id"], q, sl=price if kind == "sl" else None,
                                    tp=price if kind == "tp" else None, qprec=self.qty_prec, pprec=self.px_prec)
        oid = str((r or {}).get("orderId") or "") or None
        self._set_oid(t, kind, oid)
        try:
            time.sleep(0.3)
            pend = self.pend(t["position_id"])
            if not oid or oid not in {str(o.get("id")) for o in pend}:
                self._set_oid(t, kind, None); self.adopt_ids(t, pend)
        except Exception as e:
            log.warning("vérif id %s #%s: %s", kind, t["id"], e)
        self.db.ev("tpsl_placed", {"trade": t["id"], "kind": kind, "price": price, "qty": q,
                                   "order": t.get(f"{kind}_order_id")}, level="WARNING")

    def _sync_kind(self, t, kind, price, qty, pos, pend, modify):
        """Un ordre (sl/tp) du trade. Retour : 'ok' | 'closed' | 'covered' | 'fail'.
        - ordre propre vivant : rien (modify=False) ou modification EN PLACE (même orderId) ;
        - sinon : qty plafonnée à la qty libre, jamais de doublon (TP limite déclenché en carnet = vivant)."""
        byid = {str(o.get("id")): o for o in pend}
        oid = t.get(f"{kind}_order_id")
        old = byid.get(str(oid)) if oid else None
        free = self._free_qty(t, kind, pos, pend)
        q = round_down(min(qty, free), self.qty_prec)
        if old is not None and not modify:
            return "ok"
        if old is not None:
            q = round_down(min(qty, free), self.qty_prec)
            try:
                (self.api.tpsl_modify_sl if kind == "sl" else self.api.tpsl_modify_tp)(
                    oid, price, q, self.qty_prec, self.px_prec)
                self.db.ev("tpsl_modified", {"trade": t["id"], "kind": kind, "order": oid, "price": price, "qty": q})
                return "ok"
            except BitunixError as e:
                if str(e.code) == "30004":
                    return "closed"
                log.warning("modify %s #%s: %s -> remplacement", kind, t["id"], e)
            except Exception as e:
                log.warning("modify %s #%s: %s -> remplacement", kind, t["id"], e)
            oq = self._oqty(old)
            if kind == "sl" and free - oq >= q - 1e-12 and q >= self.min_qty:
                try:   # place PUIS annule (aucune fenêtre sans SL) : possible seulement si la qty libre le permet
                    prev = oid
                    self._place_kind(t, kind, price, q)
                    try: self.api.tpsl_cancel(prev)
                    except Exception as e: log.warning("tpsl_cancel ancien %s: %s", prev, e)
                    return "ok"
                except Exception as e:
                    log.warning("pose SL avant annulation #%s: %s", t["id"], e)
            # annulation de l'ancien, CONFIRMÉE, avant toute nouvelle pose (jamais de doublon)
            try: self.api.tpsl_cancel(oid)
            except Exception as e: log.warning("tpsl_cancel %s: %s", oid, e)
            gone = False
            for _ in range(3):
                time.sleep(0.4)
                try:
                    pend = self.pend(t["position_id"])
                    if str(oid) not in {str(o.get("id")) for o in pend}:
                        gone = True; break
                except Exception as e:
                    log.warning("tpsl_pending: %s", e)
            if not gone:
                log.error("#%s ancien %s %s toujours actif : pas de nouvelle pose", t["id"], kind, oid)
                return "fail"
            self._set_oid(t, kind, None)
            free = self._free_qty(t, kind, pos, pend)
            q = round_down(min(qty, free), self.qty_prec)
        if kind == "tp":
            px = self.price
            if px and ((t["side"] == "LONG" and px >= price) or (t["side"] == "SHORT" and px <= price)):
                log.warning("#%s TP %s déjà dépassé (prix %s) : non re-posé", t["id"], price, px); return "fail"
            if self._tp_limit_live(t):
                return "ok"
        if q < self.min_qty:
            if self._covered(kind, pos, pend):
                self.db.ev("tpsl_covered", {"trade": t["id"], "kind": kind, "pos": pos}, level="WARNING"); return "covered"
            self.db.ev("tpsl_no_free_qty", {"trade": t["id"], "kind": kind, "pos": pos, "free": free}, level="WARNING")
            return "fail"
        if q + 1e-12 < qty:
            self.db.ev("tpsl_qty_capped", {"trade": t["id"], "kind": kind, "qty": qty, "capped": q}, level="WARNING")
        try:
            self._place_kind(t, kind, price, q)
            return "ok"
        except BitunixError as e:
            c = str(e.code)
            if c == "30004":
                return "closed"
            if c == "30038":   # TP/SL > taille de position : déjà couvert ?
                try:
                    pend = self.pend(t["position_id"]); pos = self.pos_qty(t["position_id"])
                except Exception:
                    return "fail"
                if pos <= 0:
                    return "closed"
                self.adopt_ids(t, pend)
                if self.own_tpsl(t, [o for o in pend if self._kind(o) == kind]) or self._covered(kind, pos, pend):
                    self.db.ev("tpsl_covered", {"trade": t["id"], "kind": kind, "pos": pos, "err": "30038"}, level="WARNING")
                    return "covered"
            log.error("pose %s #%s: %s", kind, t["id"], e); return "fail"
        except Exception as e:
            log.error("pose %s #%s: %s", kind, t["id"], e); return "fail"

    def _covered(self, kind, pos, pend):
        return sum(self._oqty(o) for o in pend if self._kind(o) == kind) >= pos - 1e-9

    def _sync(self, t, qty, sl, tp, modify, kinds=("sl", "tp")):
        """Relit position + ordres en attente, puis synchronise. Retour {kind: statut} ou 'closed'."""
        if not t.get("position_id"):
            return {k: "fail" for k in kinds}
        try:
            pos = self.pos_qty(t["position_id"]); pend = self.pend(t["position_id"])
        except Exception as e:
            log.warning("sync #%s: %s", t["id"], e); return {k: "fail" for k in kinds}
        if pos <= 0:
            return "closed"
        self.adopt_ids(t, pend)
        out = {}
        for kind in kinds:
            r = self._sync_kind(t, kind, sl if kind == "sl" else tp, qty, pos, pend, modify)
            if r == "closed":
                return "closed"
            out[kind] = r
            try: pend = self.pend(t["position_id"])
            except Exception: pass
        return out

    def ensure_tpsl(self, t, kinds=("sl", "tp")):
        """Pose les ordres manquants du trade (ceux encore vivants ne sont jamais doublés).
        Retour : 'ok' | 'closed' | 'fail'."""
        t = self._fresh(t)
        if not t or t["status"] != "OPEN":
            return "closed"
        r = self._sync(t, t["qty"], t["sl"], t["tp"], modify=False, kinds=kinds)
        if r == "closed":
            return "closed"
        return "ok" if all(v in ("ok", "covered") for v in r.values()) else "fail"

    def swap_tpsl(self, t, qty, sl, tp):
        """Nouveaux prix/qty pour les ordres du trade : modification EN PLACE par orderId ;
        repli : (SL) pose puis annulation si la qty libre le permet, sinon annulation confirmée puis pose."""
        t = self._fresh(t)
        r = self._sync(t, qty, sl, tp, modify=True)
        if r == "closed":
            return False, {"sl": False, "tp": False, "closed": True}
        ok = {k: v in ("ok", "covered") for k, v in r.items()}
        if r.get("tp") == "fail" and self.price and ((t["side"] == "LONG" and self.price >= tp) or
                                                     (t["side"] == "SHORT" and self.price <= tp)):
            ok["tp"] = True   # TP dépassé : géré par le suivi
        return ok["sl"] and ok["tp"], ok

    def hist_status(self, t):
        """Statut Bitunix (historique) des ordres propres du trade : {'sl': 'FILLED'|..., 'tp': ...}."""
        ids = {str(t[f"{k}_order_id"]): k for k in ("sl", "tp") if t.get(f"{k}_order_id")}
        if not ids:
            return {}
        out = {}
        for o in self.api.tpsl_history(SYMBOL) or []:
            k = ids.get(str(o.get("id")))
            if k:
                out[k] = str(o.get("status") or "").upper()
        return out

    def _own_sl_filled(self, t):
        """True si le SL propre du trade est EXÉCUTÉ (par orderId ; repli prix pour les anciens trades)."""
        try:
            if t.get("sl_order_id"):
                return "FILLED" in self.hist_status(t).get("sl", "")
            hist = self.api.tpsl_history(SYMBOL)
        except Exception as e:
            log.warning("tpsl_history: %s", e); return None
        t0 = datetime.fromisoformat(t["opened_ts"]).timestamp() * 1000 - 60000
        claimed = self._claimed(exclude=t["id"])
        for o in hist or []:
            if str(o.get("positionId")) != str(t["position_id"]) or not o.get("slPrice") or str(o.get("id")) in claimed:
                continue
            if abs(float(o["slPrice"]) - t["sl"]) < 0.06 and float(o.get("ctime") or 0) >= t0 and \
                    "FILLED" in str(o.get("status") or "").upper():
                return True
        return False

    def reconcile_closed(self, t):
        """Position absente / taille 0 / 30004 : trade clos. Prix depuis l'historique de SES ordres."""
        try: st = self.hist_status(t)
        except Exception: st = {}
        if "FILLED" in st.get("sl", ""):
            return t["sl"], "SL propre exécuté"
        if "FILLED" in st.get("tp", ""):
            return t["tp"], "TP propre exécuté"
        return self.price, "fermée sur l'exchange (SL/TP)"

    def _close_market(self, t, why):
        """Fermeture au marché de la seule qty du trade (plafonnée à la position). Retour (px, raison) ou None."""
        try:
            pos = self.pos_qty(t["position_id"])
        except Exception as e:
            log.warning("positions: %s", e); pos = t["qty"]
        if pos <= 0:
            return self.reconcile_closed(t)
        qty = round_down(min(t["qty"], pos), self.qty_prec)
        self._cancel_own(t)
        try:
            if qty >= self.min_qty:
                self.api.close_qty(t["position_id"], t["side"], qty, self.qty_prec)
        except BitunixError as e:
            if str(e.code) == "30004":
                return self.reconcile_closed(t)
            self.urgent("no_sl", f"#{t['id']} SANS SL et fermeture au marché ÉCHOUÉE: {e}", key=f"slclosefail:{t['id']}")
            return None
        except Exception as e:
            self.urgent("no_sl", f"#{t['id']} SANS SL et fermeture au marché ÉCHOUÉE: {e}", key=f"slclosefail:{t['id']}")
            return None
        self.urgent("no_sl", f"#{t['id']} {why} -> fermé au marché qty {qty}", key=f"slclose:{t['id']}")
        time.sleep(1.0)
        self.repair_tpsl(t["position_id"], exclude=t["id"])
        return self.price, "SL absent : fermé au marché"

    def _cancel_own(self, t):
        try:
            live = {str(o.get("id")) for o in self.pend(t["position_id"])}
        except Exception:
            live = None
        for k in ("sl", "tp"):
            oid = t.get(f"{k}_order_id")
            if oid and (live is None or str(oid) in live):
                try: self.api.tpsl_cancel(oid)
                except Exception as e: log.info("tpsl_cancel %s: %s", oid, e)

    def fix_missing_sl(self, t, live_pos=None):
        """SL propre absent -> (a) exécuté ? trade clos ; (b) sinon re-pose (qty libre, sans doublon) ;
        (c) prix au-delà du SL ou 2 échecs -> fermeture au marché de SA qty. 30004/taille 0 = position close."""
        tid = str(t["id"])
        filled = self._own_sl_filled(t)
        if filled:
            self.db.ev("sl_filled_detected", {"trade": t["id"]}, level="WARNING")
            return t["sl"], "SL propre exécuté"
        fails = self.db.get("sl_fix_fail", {}) or {}
        px = self.price
        beyond = px is None or (t["side"] == "LONG" and px <= t["sl"]) or (t["side"] == "SHORT" and px >= t["sl"])
        if not beyond and filled is not None:
            r = self.ensure_tpsl(t, ("sl",))
            if r == "closed":
                return self.reconcile_closed(t)
            if r == "ok":
                fails.pop(tid, None); self.db.set("sl_fix_fail", fails)
                self.db.ev("sl_reposed", {"trade": t["id"], "sl": t["sl"], "qty": t["qty"]}, level="WARNING")
                return None
            fails[tid] = fails.get(tid, 0) + 1; self.db.set("sl_fix_fail", fails)
            log.error("re-pose SL #%s échec %d", t["id"], fails[tid])
            if fails[tid] < 2:
                return None
        elif not beyond:
            return None   # historique illisible : on réessaie au prochain tour
        fails.pop(tid, None); self.db.set("sl_fix_fail", fails)
        return self._close_market(t, f"SL non re-posable ({'prix au-delà du SL' if beyond else '2 échecs'})")

    def close_live(self, t):
        """Ferme uniquement la part de CE trade si la position exchange est partagée.
        Annule d'abord SES ordres (par id), qty plafonnée à la position, puis ré-aligne les autres trades."""
        others = [o for o in self.open_trades() if o["id"] != t["id"] and o["position_id"] == t["position_id"]]
        if not others:
            self._cancel_own(t)
            return self.api.flash_close(t["position_id"])
        pos_qty = self.pos_qty(t["position_id"])
        if not pos_qty:
            raise RuntimeError("position introuvable pour fermeture partielle")
        qty = round_down(min(t["qty"], pos_qty), self.qty_prec)
        if qty < self.min_qty:
            raise RuntimeError(f"qty de fermeture {qty} < min")
        self._cancel_own(t)
        time.sleep(0.5)
        r = self.api.close_qty(t["position_id"], t["side"], qty, self.qty_prec)
        time.sleep(1.0)
        self.repair_tpsl(t["position_id"], exclude=t["id"])
        return r

    def repair_tpsl(self, position_id, exclude=None):
        """Après la sortie/partielle d'un trade : re-pose les ordres des autres trades annulés par Bitunix
        (SYSTEM_CANCELED), chacun à SA qty, sans doubler ceux encore vivants."""
        for o in self.open_trades():
            if o["id"] == exclude or str(o["position_id"]) != str(position_id):
                continue
            if self.ensure_tpsl(o) == "fail":
                log.warning("repair #%s : re-pose incomplète (suivi au prochain tour)", o["id"])

    def replace_tpsl(self, t):
        px = self.price
        ok_side = px and ((t["side"] == "LONG" and t["sl"] < px < t["tp"]) or
                          (t["side"] == "SHORT" and t["tp"] < px < t["sl"]))
        if not ok_side:
            log.error("TP/SL de #%s non re-posables (prix %s hors sl/tp) -> fermeture de sa part", t["id"], px)
            r = self._close_market(t, "TP/SL absents, prix hors sl/tp")
            if r:
                self.close_trade(t, r[0], r[1])
            return r is not None
        r = self.ensure_tpsl(t)
        if r == "closed":
            self.close_trade(t, *self.reconcile_closed(t)); return True
        if r == "ok":
            self.db.ev("tpsl_replaced", {"trade": t["id"], "sl": t["sl"], "tp": t["tp"], "qty": t["qty"]}, level="WARNING")
            return True
        return False

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
                live_pos = {}
                for ps in self.api.positions(SYMBOL):
                    live_pos[str(ps.get("positionId"))] = live_pos.get(str(ps.get("positionId")), 0.0) + float(ps.get("qty") or 0)
            except Exception as e:
                log.warning("positions(): %s", e)
            try:
                tpsl = self.api.tpsl_pending(SYMBOL)
            except Exception as e:
                log.warning("tpsl_pending(): %s", e)
        need = set(self.db.get("need_tpsl", []) or [])
        for t in self.open_trades():
            opened_ms = datetime.fromisoformat(t["opened_ts"]).timestamp() * 1000
            seg = [b for b in bars if b["t"] + 60000 > opened_ms] or [{"h": self.price, "l": self.price}]
            hi, lo = max(b["h"] for b in seg), min(b["l"] for b in seg)
            exit_px = reason = None
            if PARTIAL_TP and t.get("init_sl") and not t.get("partial_done"):
                try:
                    if self.partial_step(t, hi, lo):
                        continue   # TP/SL re-posés pour la qty restante : suivi au prochain tour
                except Exception as e:
                    log.exception("partielle #%s: %s", t["id"], e)
            if REAL_ORDERS and t["id"] in need:   # re-pose TP/SL après partielle en échec
                fails = self.db.get("need_tpsl_fails", {}) or {}
                ok, part = self.swap_tpsl(t, t["qty"], t["sl"], t["tp"])
                if part.get("closed"):
                    need.discard(t["id"]); self.db.set("need_tpsl", sorted(need)); fails.pop(str(t["id"]), None)
                    self.db.set("need_tpsl_fails", fails)
                    self.close_trade(t, *self.reconcile_closed(t)); continue
                if ok:
                    need.discard(t["id"]); self.db.set("need_tpsl", sorted(need)); fails.pop(str(t["id"]), None)
                else:
                    fails[str(t["id"])] = fails.get(str(t["id"]), 0) + 1
                    if fails[str(t["id"])] >= 3:   # repli : fermeture au marché de la qty restante
                        need.discard(t["id"]); self.db.set("need_tpsl", sorted(need)); fails.pop(str(t["id"]), None)
                        r = self._close_market(t, "SL BE non posable (3 échecs)")
                        if r:
                            self.db.set("need_tpsl_fails", fails)
                            self.close_trade(t, r[0], r[1]); continue
                self.db.set("need_tpsl_fails", fails)
                continue
            if REAL_ORDERS and time.time() < float((self.db.get("partial_guard", {}) or {}).get(str(t["id"]), 0)):
                continue   # juste après la partielle : TP/SL en cours de remplacement
            if REAL_ORDERS:
                pid = str(t["position_id"]) if t["position_id"] else None
                if live_pos is not None and pid and live_pos.get(pid, 0.0) <= 0:
                    exit_px, reason = self.reconcile_closed(t)
                elif tpsl is not None and live_pos is not None and pid and time.time() * 1000 - opened_ms > 90000:
                    # v4.2.2 : suivi par orderId (plus de rapprochement par prix)
                    self.adopt_ids(t, tpsl)
                    mine = self.own_tpsl(t, tpsl)
                    has_sl = any(o.get("slPrice") for o in mine)
                    has_tp = any(o.get("tpPrice") for o in mine)
                    if not (has_sl and has_tp):
                        try: st = self.hist_status(t)
                        except Exception as e: log.warning("tpsl_history: %s", e); st = None
                        if st is None:
                            pass   # historique illisible : on réessaie au prochain tour
                        elif not has_sl and "FILLED" in st.get("sl", ""):
                            exit_px, reason = t["sl"], "SL propre exécuté"
                        elif not has_tp and "FILLED" in st.get("tp", ""):
                            if self._tp_limit_live(t):
                                continue   # TP limite déclenché, en carnet : SL toujours là, on attend
                            exit_px, reason = t["tp"], "TP propre exécuté"
                        else:
                            # annulé (ex. SYSTEM_CANCELED après partielle/sortie d'un autre trade) ou inconnu :
                            # re-pose de SES ordres, à SA qty, sans doublon
                            if not has_sl:
                                r = self.fix_missing_sl(t, live_pos)
                                if r:
                                    exit_px, reason = r
                            if exit_px is None and not has_tp:
                                if self.ensure_tpsl(t, ("tp",)) == "closed":
                                    exit_px, reason = self.reconcile_closed(t)
                                else:
                                    self.db.ev("tp_checked", {"trade": t["id"]})
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

    # -- v4.2 : clôture partielle à 1R puis SL au break-even + frais
    def r1_price(self, t):
        sg = 1 if t["side"] == "LONG" else -1
        return round(t["entry"] + sg * PARTIAL_R * abs(t["entry"] - t["init_sl"]), self.px_prec)

    def be_price(self, t):
        sg = 1 if t["side"] == "LONG" else -1
        return round(t["entry"] * (1 + sg * BE_FEE_PCT / 100), self.px_prec)

    def partial_qtys(self, t):
        pq = round_down(t["qty"] * PARTIAL_FRAC, self.qty_prec)
        rest = round_down(t["qty"] - pq, self.qty_prec)
        return (pq, rest) if pq >= self.min_qty and rest >= self.min_qty else (0.0, t["qty"])

    def place_partial_limit(self, t):
        """Ordre limite de clôture (qty partielle) au prix 1R, attaché à la position (hedge : par positionId)."""
        pq, _ = self.partial_qtys(t)
        if not pq or not t["position_id"]:
            return
        cid = f"v4p{t['id']}{secrets.token_hex(3)}"
        body = {"symbol": SYMBOL, "side": "BUY" if t["side"] == "LONG" else "SELL", "tradeSide": "CLOSE",
                "positionId": str(t["position_id"]), "qty": fmt(pq, self.qty_prec), "orderType": "LIMIT",
                "price": fmt(self.r1_price(t), self.px_prec), "effect": "GTC", "clientId": cid}
        try:
            self.api.place_order(body)
            self.db.x("UPDATE trades SET partial_cid=? WHERE id=?", (cid, t["id"]))
            self.db.ev("partial_limit_placed", {"trade": t["id"], "qty": pq, "px": body["price"], "cid": cid})
        except Exception as e:   # repli : partielle au marché quand le prix touche 1R
            self.db.ev("partial_limit_failed", {"trade": t["id"], "err": str(e)}, level="WARNING")

    def partial_step(self, t, hi, lo):
        """Retourne True si la partielle vient d'être exécutée (trade mis à jour)."""
        sg = 1 if t["side"] == "LONG" else -1
        r1 = self.r1_price(t)
        pq, rest = self.partial_qtys(t)
        if not pq:
            self.db.x("UPDATE trades SET partial_done=2 WHERE id=?", (t["id"],)); return False   # qty trop petite
        if not REAL_ORDERS:
            touched = (hi >= r1) if sg > 0 else (lo <= r1)
            if not touched or ((lo <= t["sl"]) if sg > 0 else (hi >= t["sl"])):
                return False   # SL et 1R dans la même bougie : prudence, le SL prime
            self.after_partial(t, r1, pq, rest, maker=True); return True
        cid, filled, fpx = t.get("partial_cid"), 0.0, r1
        if cid:
            try:
                det = self.api.order_detail(cid) or {}
            except Exception as e:
                log.warning("order_detail partielle %s: %s", cid, e); return False
            st = str(det.get("status") or "").upper().strip("_")
            filled = round_down(float(det.get("tradeQty") or 0), self.qty_prec)
            try: fpx = float(det.get("avgPrice") or det.get("price") or r1)
            except (TypeError, ValueError): fpx = r1
            if st == "FILLED" or (filled >= self.min_qty and "CANCEL" in st):
                pq, rest = filled, round_down(t["qty"] - filled, self.qty_prec)
                self.after_partial(t, fpx, pq, rest, maker=True); return True
            if "CANCEL" in st:   # annulée sans remplissage -> repli marché
                self.db.x("UPDATE trades SET partial_cid=NULL WHERE id=?", (t["id"],)); t["partial_cid"] = None
            else:
                return False     # en attente (NEW / PART_FILLED)
        px = self.price
        if not px or not ((px >= r1) if sg > 0 else (px <= r1)):
            return False
        self.api.close_qty(t["position_id"], t["side"], pq, self.qty_prec)   # repli : marché
        self.after_partial(t, px, pq, rest, maker=False)
        return True

    def after_partial(self, t, px, pq, rest, maker):
        sg = 1 if t["side"] == "LONG" else -1
        fees = (MAKER_FEE_RATE if maker else FEE_RATE) * pq * px + (MAKER_FEE_RATE if (FEE_MODEL_MAKER and ENTRY_POST_ONLY)
                                                                    else FEE_RATE) * pq * t["entry"]
        ppnl = sg * (px - t["entry"]) * pq - fees
        new_sl = self.be_price(t) if BE_AFTER_PARTIAL else t["sl"]
        self.db.x("UPDATE trades SET partial_done=1, partial_qty=?, partial_px=?, partial_pnl=?, partial_fees=?, "
                  "qty=?, sl=?, margin=? WHERE id=?", (pq, round(px, self.px_prec), round(ppnl, 4), round(fees, 4),
                                                        rest, new_sl, round(rest * t["entry"] / LEVERAGE, 3), t["id"]))
        if not REAL_ORDERS:
            self.db.set("paper_equity", round(self.db.get("paper_equity", START_EQUITY) + ppnl, 4))
        else:
            g = self.db.get("partial_guard", {}) or {}
            g[str(t["id"])] = time.time() + 45; self.db.set("partial_guard", g)
            # v4.2.2 : SES ordres (orderId) modifiés en place à la qty restante ; jamais de doublon
            ok = False
            for _ in range(2):
                ok, part = self.swap_tpsl(t, rest, new_sl, t["tp"])
                if ok or part.get("closed"):
                    break
                time.sleep(1.0)
            if not ok and not part.get("closed"):
                need = set(self.db.get("need_tpsl", []) or []); need.add(t["id"]); self.db.set("need_tpsl", sorted(need))
                self.urgent("no_sl", f"#{t['id']} partielle faite mais SL BE non posé : nouvel essai", key=f"nosl:{t['id']}")
            time.sleep(0.5)
            self.repair_tpsl(t["position_id"], exclude=t["id"])
        self.db.ev("partial", {"trade": t["id"], "qty": pq, "px": px, "pnl": round(ppnl, 4), "rest": rest, "sl": new_sl})
        self.alert.send({"type": "partial", "trade_id": t["id"], "timeframe": t["timeframe"], "side": t["side"],
                         "price": round(px, 1), "qty": pq, "pnl": round(ppnl, 4),
                         "why": f"#{t['id']} {t['timeframe']} {t['side']} partielle {pq} @{px:.1f} pnl {ppnl:+.4f}, "
                                f"SL -> {new_sl} (BE+frais), reste {rest}"}, key=f"partial:{t['id']}")

    # -- v4.2 : filtre Kronos 1h (lecture seule, jamais bloquant en cas d'échec)
    def kronos_state(self):
        c = getattr(self, "_kronos", None)
        if c and time.time() - c["t"] < 600:
            return c
        c = {"t": time.time(), "on": False, "acc": None, "n": 0}
        try:
            k = sqlite3.connect(f"file:{KRONOS_DB}?mode=ro", uri=True, timeout=2)
            n, ok = k.execute("SELECT COUNT(*), SUM(correct=1) FROM predictions WHERE timeframe='1h' "
                              "AND correct IS NOT NULL").fetchone()
            k.close()
            c["n"], c["acc"] = n or 0, (ok or 0) / n if n else None
            c["on"] = bool(KRONOS_FILTER and n and n >= KRONOS_MIN_N and c["acc"] >= KRONOS_MIN_ACC)
        except Exception as e:
            log.info("kronos indisponible (ignoré): %s", e)
        if c["on"] != (getattr(self, "_kronos", None) or {}).get("on", None):
            self.db.ev("kronos_state", {"on": c["on"], "acc": c["acc"], "n": c["n"]})
        self._kronos = c
        return c

    def kronos_veto(self, side):
        try:
            if not KRONOS_FILTER or not self.kronos_state()["on"]:
                return None
            k = sqlite3.connect(f"file:{KRONOS_DB}?mode=ro", uri=True, timeout=2)
            r = k.execute("SELECT created_at, pred_direction, pred_change_pct FROM predictions WHERE timeframe='1h' "
                          "ORDER BY id DESC LIMIT 1").fetchone()
            k.close()
            if not r:
                return None
            age_h = (time.time() - datetime.fromisoformat(r[0]).replace(tzinfo=timezone.utc).timestamp()) / 3600
            if age_h > KRONOS_MAX_AGE_H:
                return None
            d = str(r[1] or "").upper()
            if (side == "LONG" and d == "DOWN") or (side == "SHORT" and d == "UP"):
                return f"kronos 1h {d} ({r[2]:+.2f}%) contre {side}"
        except Exception as e:
            log.info("kronos veto ignoré: %s", e)
        return None

    # -- v4.2 : auto-kill setup x TF -> paper (shadow) puis réactivation
    def killed(self, tf, setup):
        return AUTO_KILL and tf in TRADE_TIMEFRAMES and f"{setup}|{tf}" in (self.db.get("autokill", {}) or {})

    def shadow_open(self, tf, s, p, sid):
        if self.db.q("SELECT 1 FROM shadow_trades WHERE status='OPEN' AND timeframe=? AND setup=?", (tf, s["setup"])):
            return
        self.db.x("INSERT INTO shadow_trades(opened_ts,timeframe,setup,side,entry,qty,sl,tp,init_sl,status,signal_id,"
                  "max_hold_h) VALUES(?,?,?,?,?,?,?,?,?,'OPEN',?,?)",
                  (now_iso(), tf, s["setup"], s["side"], p["price"], p["qty"], p["sl"], p["tp"], p["sl"], sid,
                   TF_MAX_HOLD_H.get(tf, 24)))

    def manage_shadow(self, hi_lo_bars):
        for t in self.db.q("SELECT * FROM shadow_trades WHERE status='OPEN'"):
            opened_ms = datetime.fromisoformat(t["opened_ts"]).timestamp() * 1000
            seg = [b for b in hi_lo_bars if b["t"] + 60000 > opened_ms] or [{"h": self.price, "l": self.price}]
            hi, lo = max(b["h"] for b in seg), min(b["l"] for b in seg)
            sg = 1 if t["side"] == "LONG" else -1
            q = t["qty"] or 0
            if PARTIAL_TP and not t["partial_done"]:
                r1 = t["entry"] + sg * PARTIAL_R * abs(t["entry"] - t["init_sl"])
                if ((hi >= r1) if sg > 0 else (lo <= r1)) and not ((lo <= t["sl"]) if sg > 0 else (hi >= t["sl"])):
                    pq = q * PARTIAL_FRAC
                    ppnl = sg * (r1 - t["entry"]) * pq - fee_estimate(pq, r1)
                    nsl = t["entry"] * (1 + sg * BE_FEE_PCT / 100) if BE_AFTER_PARTIAL else t["sl"]
                    self.db.x("UPDATE shadow_trades SET partial_done=1, partial_pnl=?, qty=?, sl=? WHERE id=?",
                              (ppnl, q - pq, nsl, t["id"]))
                    continue
            ex = None
            if (lo <= t["sl"]) if sg > 0 else (hi >= t["sl"]): ex = (t["sl"], "SL")
            elif (hi >= t["tp"]) if sg > 0 else (lo <= t["tp"]): ex = (t["tp"], "TP")
            elif (time.time() * 1000 - opened_ms) / 3.6e6 >= (t["max_hold_h"] or 24): ex = (self.price, "temps max")
            if ex:
                pnl = sg * (ex[0] - t["entry"]) * q - fee_estimate(q, ex[0]) + (t["partial_pnl"] or 0)
                self.db.x("UPDATE shadow_trades SET status='CLOSED', closed_ts=?, exit=?, pnl=?, close_reason=? WHERE id=?",
                          (now_iso(), ex[0], round(pnl, 4), ex[1], t["id"]))

    def autokill_check(self):
        if not AUTO_KILL or time.time() - float(self.db.get("autokill_last", 0) or 0) < 1800:
            return
        self.db.set("autokill_last", time.time())
        now = time.time()
        since = datetime.fromtimestamp(now - AUTO_KILL_DAYS * 86400, timezone.utc).isoformat(timespec="seconds")
        killed = self.db.get("autokill", {}) or {}
        reen = self.db.get("autokill_reenabled", {}) or {}
        events = []
        rows = self.db.q("SELECT timeframe, setup, pnl, closed_ts FROM trades WHERE status='CLOSED' AND mode=? "
                         "AND closed_ts>=?", (self.mode, since))
        stats = {}
        killed = {k: v for k, v in killed.items() if k.split("|")[1] in TRADE_TIMEFRAMES}
        for r in rows:
            if r["timeframe"] not in TRADE_TIMEFRAMES:
                continue   # TF non tradé (ex. 1m) : rien à couper
            key = f"{r['setup']}|{r['timeframe']}"
            if r["closed_ts"] < reen.get(key, ""):
                continue   # après réactivation : on ne re-juge que les nouveaux trades
            s = stats.setdefault(key, [0, 0, 0.0]); s[0] += 1; s[1] += (r["pnl"] or 0) > 0; s[2] += r["pnl"] or 0
        for key, (n, w, pnl) in stats.items():
            if key in killed or n < AUTO_KILL_MIN_N:
                continue
            if pnl < 0 and w / n < AUTO_KILL_MAX_WR:
                killed[key] = {"since": now_iso(), "n": n, "wr": round(w / n, 2), "pnl": round(pnl, 3)}
                events.append(f"AUTO-KILL {key} -> paper : {n} trades/{AUTO_KILL_DAYS:g}j, WR {w / n:.0%}, pnl {pnl:+.3f}")
        for key, info in list(killed.items()):
            t0 = datetime.fromisoformat(info["since"]).timestamp()
            if now - t0 < AUTO_KILL_DAYS * 86400:
                continue
            setup, tf = key.split("|")
            sh = self.db.q("SELECT COUNT(*) n, COALESCE(SUM(pnl),0) p FROM shadow_trades WHERE status='CLOSED' AND "
                           "setup=? AND timeframe=? AND closed_ts>=? AND closed_ts>=?",
                           (setup, tf, info["since"], since))[0]
            if sh["n"] and sh["p"] > 0:
                killed.pop(key); reen[key] = now_iso()
                events.append(f"RÉACTIVÉ {key} : paper {sh['n']} trades pnl {sh['p']:+.3f} sur {AUTO_KILL_DAYS:g}j")
        self.db.set("autokill", killed); self.db.set("autokill_reenabled", reen)
        for ev in events:
            self.db.ev("autokill", {"why": ev}, level="WARNING")
        if events:   # 1 seul événement groupé (récap)
            self.alert.send({"type": "autokill", "why": " | ".join(events), "killed": sorted(killed)}, key="autokill")

    def close_trade(self, t, px, reason):
        sgn = 1 if t["side"] == "LONG" else -1
        if REAL_ORDERS and t.get("partial_cid") and not t.get("partial_done"):
            try: self.api.cancel_orders(SYMBOL, client_ids=[t["partial_cid"]])   # partielle limite orpheline
            except Exception as e: log.info("cancel partielle %s: %s", t["partial_cid"], e)
        if REAL_ORDERS and t.get("position_id"):
            self._cancel_own(t)   # v4.2.2 : ordre restant (ex. TP après SL) agirait sur la qty des autres trades
        fees = FEE_RATE * t["qty"] * (t["entry"] + px) + float(t.get("partial_fees") or 0)
        pnl = sgn * (px - t["entry"]) * t["qty"] - FEE_RATE * t["qty"] * (t["entry"] + px) + float(t.get("partial_pnl") or 0)
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
                         "pnl": round(pnl, 4), "reason": reason + (" (après partielle)" if t.get("partial_done") else ""),
                         "trade_id": t["id"], "equity": round(eq, 2),
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
        if URGENT_DD_PCT > 0 and d0 and (d0 - eq) / d0 * 100 > URGENT_DD_PCT and self.db.get("dd_alert_day") != day:
            self.db.set("dd_alert_day", day)
            self.urgent("drawdown", f"drawdown du jour {(d0 - eq) / d0 * 100:.1f}% > {URGENT_DD_PCT}% "
                                    f"(début {d0:.2f} -> {eq:.2f})", key=f"dd:{day}")
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
            if DIGEST_TEXT:
                self.alert.send({"type": "digest", "format": "text", "text": self.digest_text(st),
                                 "equity": st["equity"], "price": self.price, "open_positions": len(st["open"])},
                                force=True)
                return
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
        if AUTO_KILL:
            try:
                self.manage_shadow(list(self.candles.get("1m", [])[-3:]) + ([self.live_1m] if getattr(self, "live_1m", None) else []))
                self.autokill_check()
            except Exception as e:
                log.exception("auto-kill: %s", e)
        self.check_global()
        self.digest()
        self.alert.tick(self)
        self.db.set("heartbeat", {"ts": now_iso(), "price": self.price, "equity": round(self.equity(), 3)})

    def run(self, once=False):
        self.init_exchange()
        self.db.ev("start", {"mode": self.mode, "auto_trade": AUTO_TRADE, "tfs": TIMEFRAMES, "lev": LEVERAGE,
                             "risk_pct": RISK_PCT, "webhook": bool(WEBHOOK_URL), "version": "4.2.2",
                             "rr": RR, "partial": PARTIAL_TP, "max_fee_share": MAX_FEE_SHARE, "atr_min": ATR_MIN_PCT,
                             "vwap_obv": VWAP_OBV, "kronos": KRONOS_FILTER, "auto_kill": AUTO_KILL,
                             "max_per_tf": MAX_PER_TF})
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
                try: self.urgent("api_error", f"step: {e}", key=f"step:{type(e).__name__}")
                except Exception: log.exception("urgent()")
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
