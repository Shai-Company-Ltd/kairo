"""trading-v4.3 — flux de marché externes utilisés comme FILTRES (fail-open).
Chaque flux : timeout court, cache, erreurs journalisées ; un flux en panne => filtre neutre (jamais de blocage global).
Binance (prioritaire, accessible depuis le VPS) avec repli Bybit / OKX."""
import time, json, threading, logging, statistics
from datetime import datetime, timezone, timedelta
import requests

log = logging.getLogger("feeds")
UA = {"User-Agent": "trading-v4.3/feeds"}
TO = 6

def _get(url, params=None, timeout=TO):
    r = requests.get(url, params=params, timeout=timeout, headers=UA)
    r.raise_for_status()
    return r.json()

class Feeds:
    # nom -> (fonction, période de rafraîchissement s, durée de validité s)
    def __init__(self, symbol="BTCUSDT", api=None, ev=None):
        self.symbol, self.api, self.ev = symbol, api, ev
        self.c = {}          # nom -> {"ok", "ts", "data", "err", "src"}
        self.lock = threading.Lock()
        self.spreads = []    # (ts, spread) 1h glissante
        self.specs = {
            "funding": (self._funding, 60, 900), "oi": (self._oi, 60, 900), "ls": (self._ls, 300, 3600),
            "taker": (self._taker, 60, 900), "liq": (self._liq, 30, 300), "book": (self._book, 15, 120),
            "options": (self._options, 900, 7200), "coinbase": (self._coinbase, 60, 600),
            "etf": (self._etf, 3600, 6 * 3600), "stables": (self._stables, 3600, 6 * 3600),
            "netflow": (self._netflow, 3600, 6 * 3600), "macro": (self._macro, 1800, 12 * 3600),
        }
        self._next = {k: 0 for k in self.specs}
        self._stop = False

    # ------------------------------------------------------------ boucle
    def start(self):
        t = threading.Thread(target=self._loop, name="feeds", daemon=True); t.start(); return t

    def _loop(self):
        while not self._stop:
            now = time.time()
            for k, (fn, per, _) in self.specs.items():
                if now >= self._next[k]:
                    self._next[k] = now + per
                    self.refresh(k)
            time.sleep(5)

    def refresh(self, k):
        fn = self.specs[k][0]
        try:
            data, src = fn()
            with self.lock:
                self.c[k] = {"ok": True, "ts": time.time(), "data": data, "err": None, "src": src}
        except Exception as e:
            with self.lock:
                old = self.c.get(k, {})
                self.c[k] = dict(old, ok=False, err=str(e)[:200], ts_err=time.time())
            log.warning("feed %s indisponible: %s", k, str(e)[:160])

    def get(self, k):
        """Donnée fraîche ou None (fail-open)."""
        with self.lock:
            x = self.c.get(k)
        if not x or "data" not in x or x.get("data") is None:
            return None
        if time.time() - x.get("ts", 0) > self.specs[k][2]:
            return None
        return x["data"]

    def status(self):
        with self.lock:
            return {k: {"ok": bool(v.get("ok")), "src": v.get("src"), "age_s": int(time.time() - v.get("ts", 0)) if v.get("ts") else None,
                        "err": v.get("err")} for k, v in self.c.items()}

    # ------------------------------------------------------------ flux
    def _funding(self):
        try:
            d = _get("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": self.symbol})
            return {"rate": float(d["lastFundingRate"]), "mark": float(d["markPrice"]), "index": float(d["indexPrice"])}, "binance"
        except Exception as e:
            try:
                d = _get("https://api.bybit.com/v5/market/tickers", {"category": "linear", "symbol": self.symbol})["result"]["list"][0]
                return {"rate": float(d["fundingRate"]), "mark": float(d["markPrice"]), "index": float(d["indexPrice"])}, "bybit"
            except Exception:
                fr = _get("https://www.okx.com/api/v5/public/funding-rate", {"instId": "BTC-USDT-SWAP"})["data"][0]
                mk = _get("https://www.okx.com/api/v5/public/mark-price", {"instType": "SWAP", "instId": "BTC-USDT-SWAP"})["data"][0]
                return {"rate": float(fr["fundingRate"]), "mark": float(mk["markPx"]), "index": None}, "okx"

    def _oi(self):
        """OI 15m : variation sur 1h (4 périodes)."""
        try:
            d = _get("https://fapi.binance.com/futures/data/openInterestHist", {"symbol": self.symbol, "period": "15m", "limit": 6})
            xs = [float(x["sumOpenInterest"]) for x in d]; src = "binance"
        except Exception as e:
            try:
                d = _get("https://api.bybit.com/v5/market/open-interest", {"category": "linear", "symbol": self.symbol,
                                                                          "intervalTime": "15min", "limit": 6})["result"]["list"]
                xs = [float(x["openInterest"]) for x in reversed(d)]; src = "bybit"
            except Exception:
                d = _get("https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume", {"ccy": "BTC", "period": "5m"})["data"][:16]
                xs = [float(x[1]) for x in reversed(d)][::3]; src = "okx"   # 15 min
        return {"oi": xs[-1], "chg_1h_pct": (xs[-1] / xs[-5] - 1) * 100 if len(xs) >= 5 and xs[-5] else 0.0}, src

    def _ls(self):
        """Ratios long/short 1h + z-score ~20 j."""
        def z(xs):
            if len(xs) < 50: return 0.0
            m, sd = statistics.mean(xs[:-1]), statistics.pstdev(xs[:-1]) or 1e-9
            return (xs[-1] - m) / sd
        try:
            top = [float(x["longShortRatio"]) for x in _get("https://fapi.binance.com/futures/data/topLongShortPositionRatio",
                                                             {"symbol": self.symbol, "period": "1h", "limit": 500})]
            acc = [float(x["longShortRatio"]) for x in _get("https://fapi.binance.com/futures/data/topLongShortAccountRatio",
                                                             {"symbol": self.symbol, "period": "1h", "limit": 2})]
            glob = [float(x["longShortRatio"]) for x in _get("https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
                                                              {"symbol": self.symbol, "period": "1h", "limit": 500})]
            return {"top_pos": top[-1], "top_acc": acc[-1], "global": glob[-1], "z_top": z(top), "z_global": z(glob)}, "binance"
        except Exception as e:
            try:
                d = _get("https://api.bybit.com/v5/market/account-ratio", {"category": "linear", "symbol": self.symbol,
                                                                          "period": "1h", "limit": 500})["result"]["list"]
                g = [float(x["buyRatio"]) / max(float(x["sellRatio"]), 1e-9) for x in reversed(d)]; src = "bybit"
            except Exception:
                d = _get("https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio", {"ccy": "BTC", "period": "1H"})["data"]
                g = [float(x[1]) for x in reversed(d)]; src = "okx"
            return {"top_pos": None, "top_acc": None, "global": g[-1], "z_top": z(g), "z_global": z(g)}, src

    def _taker(self):
        """Proxy CVD : ratio volume taker acheteur/vendeur sur la dernière heure (4 × 15m)."""
        try:
            d = _get("https://fapi.binance.com/futures/data/takerlongshortRatio", {"symbol": self.symbol, "period": "15m", "limit": 4})
            b = sum(float(x["buyVol"]) for x in d); s = sum(float(x["sellVol"]) for x in d); src = "binance"
        except Exception as e:
            d = _get("https://www.okx.com/api/v5/rubik/stat/taker-volume", {"ccy": "BTC", "instType": "CONTRACTS", "period": "5m"})["data"][:12]
            s = sum(float(x[1]) for x in d); b = sum(float(x[2]) for x in d); src = f"okx ({e.__class__.__name__})"
        return {"buy_sell": b / s if s else 1.0, "delta": b - s}, src

    def _liq(self):
        """Liquidations OKX BTC-USDT-SWAP (REST public) : somme notionnelle 5 min par côté liquidé."""
        d = _get("https://www.okx.com/api/v5/public/liquidation-orders",
                 {"instType": "SWAP", "uly": "BTC-USDT", "state": "filled", "limit": 100})["data"]
        now = time.time() * 1000; longs = shorts = 0.0
        for blk in d:
            for x in blk.get("details", []):
                if now - float(x["ts"]) > 300000: continue
                usd = float(x["sz"]) * 0.01 * float(x["bkPx"])     # ctVal 0.01 BTC
                if x.get("posSide") == "long" or (x.get("posSide") == "net" and x.get("side") == "sell"): longs += usd
                else: shorts += usd
        # OKX ≈ 10-15 % du marché : extrapolation ×8 pour estimer le total multi-exchanges
        return {"long_liq_5m": longs * 8, "short_liq_5m": shorts * 8, "okx_long": longs, "okx_short": shorts}, "okx x8"

    def _book(self):
        d = self.api.depth(self.symbol, 50) if self.api else None
        d = d[0] if isinstance(d, list) and d and isinstance(d[0], dict) and "bids" in d[0] else d
        bids = [(float(p), float(q)) for p, q, *_ in d["bids"]]; asks = [(float(p), float(q)) for p, q, *_ in d["asks"]]
        bb, ba = max(p for p, _ in bids), min(p for p, _ in asks); mid = (bb + ba) / 2
        bv = sum(q for p, q in bids if p >= mid * 0.998); av = sum(q for p, q in asks if p <= mid * 1.002)
        sp = ba - bb; now = time.time()
        self.spreads = [(t, s) for t, s in self.spreads if now - t < 3600] + [(now, sp)]
        med = statistics.median(s for _, s in self.spreads)
        return {"spread": sp, "spread_med_1h": med, "bid_share": bv / (bv + av) if bv + av else 0.5, "mid": mid}, "bitunix"

    def _options(self):
        d = _get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency", {"currency": "BTC", "kind": "option"}, 10)["result"]
        now = datetime.now(timezone.utc); exps = {}
        for x in d:
            n = x["instrument_name"].split("-")
            try: ex = datetime.strptime(n[1], "%d%b%y").replace(tzinfo=timezone.utc, hour=8)
            except ValueError: continue
            if ex <= now + timedelta(hours=12): continue
            exps.setdefault(ex, []).append((float(n[2]), n[3], float(x.get("open_interest") or 0)))
        ex = min(exps); oi = {}
        for k, t, q in exps[ex]:
            oi.setdefault(k, [0, 0])[t == "P"] += q
        pain = min(oi, key=lambda s: sum(c * max(0, s - k) + p * max(0, k - s) for k, (c, p) in oi.items()))
        big = sorted(oi, key=lambda k: -sum(oi[k]))[:8]
        # basis : futures Deribit le plus proche (> 20 j) vs index
        basis = None
        try:
            f = _get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency", {"currency": "BTC", "kind": "future"}, 10)["result"]
            best = None
            for x in f:
                n = x["instrument_name"].split("-")
                if len(n) < 2 or n[1] == "PERPETUAL": continue
                e = datetime.strptime(n[1], "%d%b%y").replace(tzinfo=timezone.utc, hour=8); days = (e - now).total_seconds() / 86400
                if days > 20 and (best is None or days < best[0]): best = (days, float(x["mark_price"]), float(x["estimated_delivery_price"]))
            if best: basis = (best[1] / best[2] - 1) * 365 / best[0] * 100
        except Exception as e:
            log.info("basis: %s", e)
        last_fri = self._monthly_expiry(now)
        return {"expiry": ex.isoformat(), "max_pain": pain, "levels": sorted(set(big + [pain])), "basis_ann_pct": basis,
                "monthly_expiry": last_fri.isoformat()}, "deribit"

    @staticmethod
    def _monthly_expiry(now):
        y, m = now.year, now.month
        for _ in range(2):
            nxt = datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc)
            d = nxt - timedelta(days=1)
            while d.weekday() != 4: d -= timedelta(days=1)
            d = d.replace(hour=8)
            if d > now: return d
            y, m = y + (m == 12), m % 12 + 1
        return d

    def _coinbase(self):
        cb = float(_get("https://api.exchange.coinbase.com/products/BTC-USD/ticker")["price"])
        try:
            ref = float(_get("https://api.binance.com/api/v3/ticker/price", {"symbol": "BTCUSDT"})["price"]); src = "coinbase/binance"
        except Exception:
            ref = float(_get("https://www.okx.com/api/v5/market/ticker", {"instId": "BTC-USDT"})["data"][0]["last"]); src = "coinbase/okx"
        return {"premium_pct": (cb / ref - 1) * 100}, src

    def _etf(self):
        """Flux nets ETF spot BTC (Farside, dernier jour publié), en M$."""
        import re
        r = requests.get("https://farside.co.uk/bitcoin-etf-flow-all-data/", timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        rows = re.findall(r"<tr>(.*?)</tr>", r.text, re.S); last = None
        for row in rows:
            cells = [re.sub("<.*?>", "", c).strip() for c in re.findall(r"<td.*?>(.*?)</td>", row, re.S)]
            if cells and re.match(r"\d{2} \w{3} \d{4}", cells[0]) and cells[-1] not in ("", "-"):
                v = cells[-1].replace(",", ""); neg = v.startswith("(")
                try: last = (cells[0], -float(v.strip("()")) if neg else float(v))
                except ValueError: pass
        if not last: raise RuntimeError("tableau Farside illisible")
        return {"date": last[0], "net_musd": last[1]}, "farside"

    def _stables(self):
        d = _get("https://stablecoins.llama.fi/stablecoincharts/all", timeout=10)
        tot = lambda x: float(x["totalCirculatingUSD"].get("peggedUSD", 0))
        return {"total_busd": tot(d[-1]) / 1e9, "chg_7d_pct": (tot(d[-1]) / tot(d[-8]) - 1) * 100}, "defillama"

    def _netflow(self):
        import os
        key = os.environ.get("CRYPTOQUANT_KEY")
        if not key:
            raise RuntimeError("désactivé : CRYPTOQUANT_KEY absente (pas de source gratuite fiable)")
        d = requests.get("https://api.cryptoquant.com/v1/btc/exchange-flows/netflow", params={"exchange": "all_exchange", "window": "day", "limit": 2},
                         headers={"Authorization": f"Bearer {key}"}, timeout=TO).json()
        return {"netflow_btc": float(d["result"]["data"][0]["netflow_total"])}, "cryptoquant"

    def _macro(self):
        d = _get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=10)
        ev = []
        for x in d:
            if x.get("impact") == "High" and x.get("country") == "USD":
                try: ev.append({"t": datetime.fromisoformat(x["date"]).timestamp(), "title": x.get("title")})
                except Exception: pass
        return {"events": ev}, "forexfactory"
