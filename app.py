# -*- coding: utf-8 -*-
"""
Backend do Terminal de Mercado — Criteria Financial Group
=========================================================
Agrega dados de mercado ao vivo e serve nos 8 endpoints que o
terminal-mercado.html ja espera:

    /api/ibov          Ibovespa (Yahoo ^BVSP)
    /api/acoes         Maiores altas/baixas da B3 (brapi.dev — precisa token)
    /api/indices       Indices globais (Yahoo)
    /api/futuros       WIN / WDO (proxy: Ibov pts + USD/BRL)
    /api/juros         Selic / CDI / IPCA (Banco Central) + Treasuries (Yahoo)
    /api/commodities   Brent, WTI, Ouro, Prata, Milho, Soja, Boi, Cafe (Yahoo)
    /api/moedas        USD / EUR / GBP / JPY vs BRL (AwesomeAPI)
    /api/cripto        BTC / ETH / SOL / XRP / BNB (Binance)

Formato de resposta (contrato com o front):
    { "ok": true, "data": { ... } }   -> front usa esses dados
    { "ok": false }                   -> front cai no snapshot embutido dele

A unica fonte que exige cadastro e a brapi (token gratis em brapi.dev/dashboard).
Configure como variavel de ambiente no servico de nuvem:  BRAPI_TOKEN
"""

import os
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # libera o fetch de qualquer origem (inclui arquivo aberto local: origin "null")

BRAPI_TOKEN = os.environ.get("BRAPI_TOKEN", "").strip()
HTTP_TIMEOUT = 12
UA = {"User-Agent": "Mozilla/5.0 (CriteriaTerminal/1.0)"}

# ──────────────────────────────────────────────────────────────────────────
# Cache simples em memoria — evita martelar as fontes e responde rapido
# (o front tem timeout de 4s). TTL por endpoint.
# ──────────────────────────────────────────────────────────────────────────
_CACHE = {}
_CACHE_LOCK = threading.Lock()
CACHE_TTL = 60  # segundos


def cached(key, ttl, producer):
    now = time.time()
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit and (now - hit[0]) < ttl:
            return hit[1]
    # produz fora do lock pra nao serializar chamadas de rede
    value = producer()
    with _CACHE_LOCK:
        # so guarda no cache se deu certo (ok=True); erros podem ser transitorios
        if value and value.get("ok"):
            _CACHE[key] = (now, value)
        else:
            # mantem o ultimo bom, se existir, em vez de cravar um erro
            hit = _CACHE.get(key)
            if hit:
                return hit[1]
    return value


# ──────────────────────────────────────────────────────────────────────────
# Helpers de fonte
# ──────────────────────────────────────────────────────────────────────────
def _yahoo_chart(symbol):
    """Fallback: API de chart do Yahoo direto (sem a lib), via requests."""
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
           "?range=5d&interval=1d")
    r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    res = r.json()["chart"]["result"][0]
    q = res["indicators"]["quote"][0]
    closes = [c for c in q.get("close", []) if c is not None]
    if not closes:
        return None, None, None
    price = float(closes[-1])
    prev = float(closes[-2]) if len(closes) >= 2 else res.get("meta", {}).get("chartPreviousClose")
    prev = float(prev) if prev else None
    change = round((price / prev - 1) * 100, 2) if prev else None

    def last_valid(arr):
        for v in reversed(arr or []):
            if v is not None:
                return float(v)
        return None

    row = {"open": last_valid(q.get("open")), "high": last_valid(q.get("high")),
           "low": last_valid(q.get("low")), "close": price, "prev": prev,
           "volume": last_valid(q.get("volume"))}
    return price, change, row


def yf_quote(symbol):
    """Cotacao do Yahoo. Tenta a lib yfinance; se falhar/rate-limitar, cai na API de chart direta.
    Retorna (price, change_pct, row)."""
    # 1) lib yfinance
    try:
        import yfinance as yf
        h = yf.Ticker(symbol).history(period="5d", interval="1d", auto_adjust=False)
        if h is not None and not h.empty:
            closes = h["Close"].dropna()
            if len(closes):
                price = float(closes.iloc[-1])
                prev = float(closes.iloc[-2]) if len(closes) >= 2 else None
                change = round((price / prev - 1) * 100, 2) if prev else None
                last = h.iloc[-1]
                nan = lambda x: float(x) if x == x else None  # NaN-safe
                row = {"open": nan(last.get("Open")), "high": nan(last.get("High")),
                       "low": nan(last.get("Low")), "close": price, "prev": prev,
                       "volume": nan(last.get("Volume"))}
                return price, change, row
    except Exception as e:
        app.logger.warning("yfinance %s falhou: %s", symbol, e)
    # 2) fallback: chart API direta
    try:
        return _yahoo_chart(symbol)
    except Exception as e:
        app.logger.warning("yahoo chart %s falhou: %s", symbol, e)
        return None, None, None


def yf_batch(symbols):
    """Busca varios simbolos do Yahoo EM PARALELO. Retorna {symbol: (price, change, row)}.
    Isso evita que as secoes com muitos tickers (indices, commodities) estourem o
    timeout de 4s do front por fazerem as chamadas em sequencia."""
    symbols = list(dict.fromkeys(symbols))  # remove duplicados, mantem ordem
    if not symbols:
        return {}
    out = {}
    with ThreadPoolExecutor(max_workers=min(10, len(symbols))) as ex:
        for sym, res in ex.map(lambda s: (s, yf_quote(s)), symbols):
            out[sym] = res
    return out


def yf_many(spec):
    """spec: lista de dicts já com fl/tk/nm/d/yahoo. Preenche price+change (em paralelo).
    Retorna a lista de items prontos pro front. Item sem cotacao vira price=None."""
    quotes = yf_batch([s["yahoo"] for s in spec])
    items = []
    got_any = False
    for s in spec:
        price, change, _ = quotes.get(s["yahoo"], (None, None, None))
        if price is not None:
            got_any = True
        item = {k: s[k] for k in ("fl", "tk", "nm") if k in s}
        item["price"] = price
        item["change"] = change
        if "d" in s:
            item["d"] = s["d"]
        if "fmt" in s:
            item["fmt"] = s["fmt"]
        items.append(item)
    return items, got_any


def bcb_sgs(serie):
    """Ultimo valor de uma serie do Banco Central (SGS). Retorna float ou None."""
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados/ultimos/1?formato=json"
    try:
        r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if data:
            return float(str(data[-1]["valor"]).replace(",", "."))
    except Exception as e:
        app.logger.warning("BCB serie %s falhou: %s", serie, e)
    return None


# ──────────────────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────────────────
@app.route("/")
def home():
    return jsonify({
        "service": "Terminal de Mercado — Criteria",
        "ok": True,
        "brapi_token": bool(BRAPI_TOKEN),
        "endpoints": ["ibov", "acoes", "indices", "futuros",
                      "juros", "commodities", "moedas", "cripto"],
    })


@app.route("/api/ibov")
def api_ibov():
    def build():
        price, change, row = yf_quote("^BVSP")
        if price is None:
            return {"ok": False}
        return {"ok": True, "data": {
            "price": round(price, 0),
            "change": change,
            "volume": row.get("volume"),
            "trades": None,
            "low": round(row["low"]) if row.get("low") else None,
            "high": round(row["high"]) if row.get("high") else None,
            "open": round(row["open"]) if row.get("open") else None,
            "prev": round(row["prev"]) if row.get("prev") else None,
        }}
    return jsonify(cached("ibov", CACHE_TTL, build))


@app.route("/api/acoes")
def api_acoes():
    def build():
        if not BRAPI_TOKEN:
            # sem token nao da pra puxar movers da B3 -> front mostra snapshot dele
            return {"ok": False}

        def movers(order):
            url = ("https://brapi.dev/api/quote/list"
                   f"?type=stock&sortBy=change&sortOrder={order}&limit=25")
            r = requests.get(url, headers={**UA, "Authorization": f"Bearer {BRAPI_TOKEN}"},
                             timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            stocks = r.json().get("stocks", []) or []
            out = []
            for it in stocks:
                vol = it.get("volume")
                # filtra ilíquidas pra nao poluir com micro caps
                if vol is None or vol < 1_000_000:
                    continue
                out.append({
                    "tk": it.get("stock"),
                    "nm": it.get("name") or it.get("stock"),
                    "p": it.get("close"),
                    "pct": it.get("change"),
                    "vol": vol,
                })
                if len(out) >= 5:
                    break
            return out

        altas = movers("desc")
        baixas = movers("asc")
        if not altas and not baixas:
            return {"ok": False}
        return {"ok": True, "data": {
            "total": len(altas) + len(baixas),
            "altas": altas,
            "baixas": baixas,
        }}
    return jsonify(cached("acoes", CACHE_TTL, build))


@app.route("/api/indices")
def api_indices():
    def build():
        spec = [
            {"fl": "🇺🇸", "tk": "SPX",  "nm": "S&amp;P 500",      "yahoo": "^GSPC",     "d": 0},
            {"fl": "🇺🇸", "tk": "DJI",  "nm": "Dow Jones",     "yahoo": "^DJI",      "d": 0},
            {"fl": "🇺🇸", "tk": "NDX",  "nm": "Nasdaq",        "yahoo": "^NDX",      "d": 0},
            {"fl": "🇯🇵", "tk": "N225", "nm": "Nikkei 225",    "yahoo": "^N225",     "d": 0},
            {"fl": "🇪🇺", "tk": "SX5E", "nm": "Euro Stoxx 50", "yahoo": "^STOXX50E", "d": 0},
            {"fl": "🇭🇰", "tk": "HSI",  "nm": "Hang Seng",     "yahoo": "^HSI",      "d": 0},
        ]
        items, got = yf_many(spec)
        return {"ok": got, "data": {"items": items}}
    return jsonify(cached("indices", CACHE_TTL, build))


@app.route("/api/futuros")
def api_futuros():
    def build():
        # WIN ~ Ibovespa em pontos ; WDO ~ dolar (USD/BRL)
        ibov_price, ibov_change, _ = yf_quote("^BVSP")
        usd_price, _, _ = yf_quote("USDBRL=X")
        items = []
        if ibov_price is not None:
            items.append({"fl": "🇧🇷", "tk": "WIN",
                          "nm": "Indice Mini (WINM26) aprox.IBOV",
                          "price": round(ibov_price), "change": ibov_change, "fmt": "pts"})
        if usd_price is not None:
            items.append({"fl": "🇺🇸", "tk": "WDO",
                          "nm": "Dolar Mini (WDOM26) USD-BRL",
                          "price": round(usd_price, 4), "change": None, "fmt": "brl"})
        return {"ok": bool(items), "data": {"items": items}}
    return jsonify(cached("futuros", CACHE_TTL, build))


@app.route("/api/juros")
def api_juros():
    def build():
        items = []
        # ── BR (Banco Central / SGS) — em paralelo ──
        with ThreadPoolExecutor(max_workers=3) as ex:
            selic, cdi, ipca = list(ex.map(bcb_sgs, [432, 4389, 433]))
        # 432 = Meta Selic Copom (% a.a.) ; 4389 = CDI anualizado ; 433 = IPCA mensal (%)
        items.append({"fl": "🇧🇷", "tk": "SELIC", "nm": "Selic Meta", "price": selic, "change": None, "d": 2})
        items.append({"fl": "🇧🇷", "tk": "CDI",   "nm": "CDI Over",   "price": cdi,   "change": None, "d": 2})
        items.append({"fl": "🇧🇷", "tk": "IPCA",  "nm": "IPCA (mes)", "price": ipca,  "change": None, "d": 2})
        # ── EUA (Treasuries via Yahoo, em paralelo) — 2A nao tem ticker limpo ──
        tre = yf_batch(["^TNX", "^TYX"])
        ust = {
            "UST2":  (None, "T-Note 2A"),
            "UST10": (tre.get("^TNX", (None, None, None))[0], "T-Note 10A"),
            "UST30": (tre.get("^TYX", (None, None, None))[0], "T-Bond 30A"),
        }
        for tk, (price, nm) in ust.items():
            items.append({"fl": "🇺🇸", "tk": tk, "nm": nm,
                          "price": round(price, 3) if price else None, "change": 0, "d": 3})
        got = any(i["price"] is not None for i in items)
        return {"ok": got, "data": {"items": items}}
    return jsonify(cached("juros", CACHE_TTL, build))


@app.route("/api/commodities")
def api_commodities():
    def build():
        spec = [
            {"fl": "🛢️", "tk": "BRENT", "nm": "Petroleo Brent", "yahoo": "BZ=F"},
            {"fl": "🛢️", "tk": "WTI",   "nm": "Petroleo WTI",   "yahoo": "CL=F"},
            {"fl": "🥇", "tk": "GOLD",  "nm": "Ouro",           "yahoo": "GC=F"},
            {"fl": "🥈", "tk": "SLVR",  "nm": "Prata",          "yahoo": "SI=F"},
            {"fl": "🌽", "tk": "CORN",  "nm": "Milho",          "yahoo": "ZC=F"},
            {"fl": "🌱", "tk": "SOYB",  "nm": "Soja",           "yahoo": "ZS=F"},
            {"fl": "🐂", "tk": "LIVE",  "nm": "Boi Gordo",      "yahoo": "LE=F"},
            {"fl": "☕", "tk": "COFF",  "nm": "Cafe Arabica",   "yahoo": "KC=F"},
        ]
        items, got = yf_many(spec)
        return {"ok": got, "data": {"items": items}}
    return jsonify(cached("commodities", CACHE_TTL, build))


@app.route("/api/moedas")
def api_moedas():
    def build():
        # Yahoo (ja confirmado funcionando no servidor) — AwesomeAPI estava dando 429.
        spec = [
            {"fl": "🇺🇸", "tk": "USD", "nm": "Dolar Comercial",  "yahoo": "USDBRL=X"},
            {"fl": "🇪🇺", "tk": "EUR", "nm": "Euro",             "yahoo": "EURBRL=X"},
            {"fl": "🇬🇧", "tk": "GBP", "nm": "Libra Esterlina",  "yahoo": "GBPBRL=X"},
            {"fl": "🇯🇵", "tk": "JPY", "nm": "Iene Japones",     "yahoo": "JPYBRL=X"},
        ]
        quotes = yf_batch([s["yahoo"] for s in spec])
        items = []
        got = False
        for s in spec:
            price, change, _ = quotes.get(s["yahoo"], (None, None, None))
            if price is not None:
                got = True
            items.append({"fl": s["fl"], "tk": s["tk"], "nm": s["nm"],
                          "bid": round(price, 4) if price else None,
                          "change": change})
        return {"ok": got, "data": {"items": items}}
    return jsonify(cached("moedas", CACHE_TTL, build))


@app.route("/api/cripto")
def api_cripto():
    def build():
        # (flag, ticker, nome, simbolo binance, id coingecko)
        meta = [
            ("₿", "BTC", "Bitcoin",  "BTCUSDT", "bitcoin"),
            ("Ξ", "ETH", "Ethereum", "ETHUSDT", "ethereum"),
            ("◎", "SOL", "Solana",   "SOLUSDT", "solana"),
            ("✕", "XRP", "XRP",      "XRPUSDT", "ripple"),
            ("◈", "BNB", "BNB",      "BNBUSDT", "binancecoin"),
        ]
        # Fonte 1: Binance public data (data-api.binance.vision) — NAO tem o
        # bloqueio geografico dos EUA que a api.binance.com tem.
        try:
            symbols = "[" + ",".join(f'"{s}"' for _, _, _, s, _ in meta) + "]"
            url = "https://data-api.binance.vision/api/v3/ticker/24hr?symbols=" + symbols
            r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            by = {d["symbol"]: d for d in r.json()}
            items = []
            for fl, tk, nm, sym, _ in meta:
                d = by.get(sym)
                if d:
                    items.append({"fl": fl, "tk": tk, "nm": nm,
                                  "price": float(d["lastPrice"]), "change1h": None,
                                  "change24h": round(float(d["priceChangePercent"]), 2)})
            if items:
                return {"ok": True, "data": {"items": items}}
        except Exception as e:
            app.logger.warning("Binance.vision falhou: %s", e)
        # Fonte 2 (fallback): CoinGecko
        try:
            ids = ",".join(cg for _, _, _, _, cg in meta)
            url = ("https://api.coingecko.com/api/v3/simple/price"
                   f"?ids={ids}&vs_currencies=usd&include_24hr_change=true")
            r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            cg = r.json()
            items = []
            for fl, tk, nm, _, cid in meta:
                d = cg.get(cid)
                if d:
                    items.append({"fl": fl, "tk": tk, "nm": nm,
                                  "price": float(d["usd"]), "change1h": None,
                                  "change24h": round(float(d.get("usd_24h_change", 0)), 2)})
            if items:
                return {"ok": True, "data": {"items": items}}
        except Exception as e:
            app.logger.warning("CoinGecko falhou: %s", e)
        return {"ok": False, "data": {"items": []}}
    return jsonify(cached("cripto", CACHE_TTL, build))


@app.route("/api/status")
def api_status():
    """Diagnostico: testa UMA chamada de cada fonte e diz o que funciona a partir
    do servidor (util pra saber o que o IP da nuvem consegue acessar)."""
    out = {"brapi_token_configurado": bool(BRAPI_TOKEN)}

    # Yahoo (ibov, indices, commodities, treasuries)
    try:
        p, _, _ = yf_quote("^BVSP")
        out["yahoo"] = {"ok": p is not None, "amostra_ibov": p}
    except Exception as e:
        out["yahoo"] = {"ok": False, "erro": str(e)[:160]}

    # brapi (altas/baixas)
    if BRAPI_TOKEN:
        try:
            r = requests.get("https://brapi.dev/api/quote/list?type=stock&limit=1",
                             headers={**UA, "Authorization": f"Bearer {BRAPI_TOKEN}"},
                             timeout=HTTP_TIMEOUT)
            out["brapi"] = {"ok": r.status_code == 200 and bool(r.json().get("stocks")),
                            "http": r.status_code}
        except Exception as e:
            out["brapi"] = {"ok": False, "erro": str(e)[:160]}
    else:
        out["brapi"] = {"ok": False, "obs": "sem BRAPI_TOKEN configurado"}

    # Banco Central (Selic/CDI/IPCA)
    try:
        s = bcb_sgs(432)
        out["banco_central"] = {"ok": s is not None, "amostra_selic": s}
    except Exception as e:
        out["banco_central"] = {"ok": False, "erro": str(e)[:160]}

    # Moedas (agora via Yahoo, junto com o resto)
    try:
        p, _, _ = yf_quote("USDBRL=X")
        out["moedas_yahoo"] = {"ok": p is not None, "amostra_usdbrl": round(p, 4) if p else None}
    except Exception as e:
        out["moedas_yahoo"] = {"ok": False, "erro": str(e)[:160]}

    # Cripto (Binance.vision)
    try:
        r = requests.get("https://data-api.binance.vision/api/v3/ticker/price?symbol=BTCUSDT",
                         headers=UA, timeout=HTTP_TIMEOUT)
        out["cripto_binance_vision"] = {"ok": r.status_code == 200, "http": r.status_code}
    except Exception as e:
        out["cripto_binance_vision"] = {"ok": False, "erro": str(e)[:160]}

    return jsonify(out)


if __name__ == "__main__":
    # execucao local opcional: python app.py
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)
