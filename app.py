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
from flask import Flask, jsonify, Response
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
    """Serve o proprio terminal — mesma origem, sem CORS e sem URL pra editar."""
    return Response(TERMINAL_HTML, mimetype="text/html")


@app.route("/health")
def health():
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


TERMINAL_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Criteria · Terminal de Mercado</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --void:#010912;--bg:#030F1D;--surface:#071826;--surf2:#0C2238;--surf3:#112A45;
  --b0:rgba(56,182,255,.04);--b1:rgba(56,182,255,.10);--b2:rgba(56,182,255,.20);--b3:rgba(56,182,255,.38);
  --ac:#38B6FF;--ac2:#1A8FD1;--ac-g:rgba(56,182,255,.12);--ac-d:rgba(56,182,255,.06);
  --gold:#E8A020;--gold-g:rgba(232,160,32,.12);
  --up:#00D47A;--up-g:rgba(0,212,122,.10);--up-t:#00FF96;
  --dn:#FF3D5A;--dn-g:rgba(255,61,90,.10);--dn-t:#FF6B82;
  --warn:#F5A623;--warn-g:rgba(245,166,35,.10);
  --t1:#D4ECFF;--t2:#6B9AB8;--t3:#2E5A78;--t4:#1A3A52;
  --sans:'Inter',system-ui,sans-serif;
  --mono:'JetBrains Mono','Fira Code',monospace;
}
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box;}
html{scroll-behavior:smooth;}
body{font-family:var(--sans);background:var(--void);color:var(--t1);font-size:13px;line-height:1.4;-webkit-font-smoothing:antialiased;min-height:100vh;overflow-x:hidden;}
body::before{content:'';position:fixed;inset:0;background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,.025) 2px,rgba(0,0,0,.025) 4px);pointer-events:none;z-index:0;}
body::after{content:'';position:fixed;top:0;left:50%;transform:translateX(-50%);width:80%;height:400px;background:radial-gradient(ellipse at 50% 0%,rgba(56,182,255,.055) 0%,transparent 70%);pointer-events:none;z-index:0;}

/* HEADER */
header{position:sticky;top:0;z-index:100;background:rgba(3,15,29,.94);backdrop-filter:blur(20px) saturate(180%);border-bottom:1px solid var(--b1);padding:0 1.5rem;height:58px;display:flex;align-items:center;justify-content:space-between;}
.hd-left{display:flex;align-items:center;gap:16px;}
.logo{display:flex;align-items:center;gap:10px;}
.logo-icon{width:40px;height:40px;background:none;border-radius:0;display:flex;align-items:center;justify-content:center;box-shadow:none;flex-shrink:0;}
.logo-icon svg{width:40px;height:40px;}
.logo-name{font-size:15px;font-weight:700;color:#fff;letter-spacing:.2px;line-height:1.1;}
.logo-sub{font-size:8px;font-weight:500;color:#1471A0;letter-spacing:2.5px;text-transform:uppercase;margin-top:2px;}
.hd-sep{width:1px;height:28px;background:var(--b1);}
.hd-ttl{font-size:13px;font-weight:600;color:var(--t1);}
.hd-dt{font-size:9.5px;color:var(--t2);margin-top:2px;}
.mkt-chip{display:flex;align-items:center;gap:6px;padding:4px 12px;border-radius:20px;font-size:10px;font-weight:700;letter-spacing:.5px;transition:all .3s;}
.mkt-chip.open{background:rgba(0,212,122,.12);border:1px solid rgba(0,212,122,.3);color:var(--up-t);}
.mkt-chip.closed{background:rgba(255,61,90,.08);border:1px solid rgba(255,61,90,.2);color:var(--dn-t);}
.mkt-chip.pre{background:rgba(245,166,35,.10);border:1px solid rgba(245,166,35,.3);color:var(--warn);}
.mkt-chip.after{background:rgba(100,100,255,.10);border:1px solid rgba(100,100,255,.3);color:#9898FF;}
.mdot{width:6px;height:6px;border-radius:50%;}
.mkt-chip.open .mdot{background:var(--up);animation:pulse 1.4s infinite;}
.mkt-chip.closed .mdot{background:var(--dn);}
.mkt-chip.pre .mdot{background:var(--warn);animation:pulse .8s infinite;}
.mkt-chip.after .mdot{background:#7070FF;}
.hd-clock{text-align:right;}
.clk-time{font-family:var(--mono);font-size:15px;font-weight:600;color:var(--t1);letter-spacing:1px;}
.clk-tz{font-size:8px;color:var(--t3);letter-spacing:1.5px;text-transform:uppercase;margin-top:1px;}

/* STATUS BAR */
.sbar{background:rgba(3,15,29,.8);border-bottom:1px solid var(--b0);padding:5px 1.5rem;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:4px;font-size:9px;color:var(--t3);font-weight:500;}
.sbar-dots{display:flex;align-items:center;gap:12px;flex-wrap:wrap;}
.si{display:flex;align-items:center;gap:5px;}
.sd{width:6px;height:6px;border-radius:50%;background:var(--t4);transition:background .3s;}
.sd.ok{background:var(--up);}.sd.ld{background:var(--warn);animation:pulse .8s infinite;}.sd.er{background:var(--dn);}
.sbar-right{font-size:8.5px;color:var(--t3);}
.sbar-right b{color:var(--t2);}

/* IBOVESPA HERO */
.ibov-hero{background:linear-gradient(135deg,#031425 0%,#041C35 50%,#031425 100%);border-bottom:1px solid var(--b1);padding:1.25rem 1.5rem;position:relative;overflow:hidden;}
.ibov-hero::before{content:'';position:absolute;top:0;right:0;width:40%;height:100%;background:radial-gradient(ellipse at 100% 50%,rgba(56,182,255,.06) 0%,transparent 70%);pointer-events:none;}
.ibov-inner{display:flex;align-items:center;gap:2rem;flex-wrap:wrap;position:relative;z-index:1;}
.ibov-main{min-width:220px;}
.ibov-label{font-size:9px;font-weight:700;letter-spacing:2.5px;color:var(--ac);text-transform:uppercase;margin-bottom:6px;display:flex;align-items:center;gap:8px;}
.ibov-label::before{content:'';display:inline-block;width:24px;height:1px;background:var(--ac);}
.ibov-price{font-family:var(--mono);font-size:42px;font-weight:700;color:#fff;letter-spacing:-1px;line-height:1;text-shadow:0 0 40px rgba(56,182,255,.2);}
.ibov-badge{display:inline-flex;align-items:center;gap:4px;padding:4px 12px;border-radius:4px;font-family:var(--mono);font-size:12px;font-weight:600;margin-top:8px;}
.ibov-badge.up{background:var(--up-g);color:var(--up-t);border:1px solid rgba(0,212,122,.25);}
.ibov-badge.dn{background:var(--dn-g);color:var(--dn-t);border:1px solid rgba(255,61,90,.25);}
.ibov-badge.neu{background:var(--ac-d);color:var(--ac);border:1px solid var(--b1);}
.ibov-stats{display:flex;flex-wrap:wrap;gap:.6rem 1.5rem;align-items:center;}
.ibov-stat{display:flex;flex-direction:column;gap:3px;}
.ibov-stat-l{font-size:8.5px;font-weight:600;color:var(--t3);letter-spacing:1.5px;text-transform:uppercase;}
.ibov-stat-v{font-family:var(--mono);font-size:13px;font-weight:600;color:var(--t1);}
.ibov-vol{background:rgba(56,182,255,.06);border:1px solid var(--b1);border-radius:8px;padding:8px 16px;}
.ibov-right{margin-left:auto;display:flex;flex-direction:column;align-items:flex-end;gap:10px;flex-shrink:0;}
.ibov-upd{font-size:8.5px;color:var(--t3);}
.refresh-btn{background:var(--ac-g);border:1px solid var(--b2);border-radius:6px;padding:7px 18px;color:var(--ac);font-family:var(--sans);font-size:10px;font-weight:700;letter-spacing:.8px;text-transform:uppercase;cursor:pointer;transition:all .2s;}
.refresh-btn:hover{background:var(--b2);border-color:var(--b3);color:#fff;box-shadow:0 0 16px rgba(56,182,255,.15);}

/* MAIN */
main{padding:1rem 1.5rem;position:relative;z-index:1;}
.sec-hd{display:flex;align-items:center;gap:10px;margin-bottom:.6rem;padding-bottom:.5rem;border-bottom:1px solid var(--b1);}
.sec-title{font-size:9px;font-weight:700;letter-spacing:2px;color:var(--t2);text-transform:uppercase;}
.sec-ts{margin-left:auto;font-size:8px;color:var(--t3);}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:.75rem;margin-bottom:.75rem;}
.g1{margin-bottom:.75rem;}

/* CARDS */
.card{background:var(--surface);border:1px solid var(--b1);border-radius:12px;overflow:hidden;transition:border-color .2s;}
.card:hover{border-color:var(--b2);}
.card-hd{display:flex;align-items:center;gap:8px;padding:9px 14px;background:rgba(0,0,0,.25);border-bottom:1px solid var(--b0);}
.card-icon{font-size:13px;line-height:1;}
.card-title{font-size:9.5px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--t2);}
.card-badge{margin-left:auto;font-size:8px;font-weight:700;letter-spacing:.5px;padding:2px 6px;border-radius:3px;}
.card-badge.ok{background:var(--up-g);color:var(--up);border:1px solid rgba(0,212,122,.25);}
.card-badge.er{background:var(--dn-g);color:var(--dn);border:1px solid rgba(255,61,90,.25);}
.card-badge.ld{background:var(--warn-g);color:var(--warn);border:1px solid rgba(245,166,35,.25);}
.card-badge.cl{background:var(--ac-d);color:var(--t2);border:1px solid var(--b1);}
.mover-hd{display:flex;align-items:center;gap:8px;padding:8px 14px;font-size:9.5px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;border-bottom:1px solid var(--b0);}
.mover-hd.up{background:rgba(0,212,122,.06);color:var(--up);}
.mover-hd.dn{background:rgba(255,61,90,.06);color:var(--dn);}
.ws-tag{margin-left:auto;font-size:7.5px;font-weight:700;letter-spacing:.5px;padding:1px 5px;border-radius:3px;background:var(--up-g);color:var(--up);border:1px solid rgba(0,212,122,.25);}

/* COLUMN HEADERS */
.col-hd{display:grid;padding:5px 14px;background:rgba(0,0,0,.2);border-bottom:1px solid var(--b0);font-size:8px;font-weight:700;letter-spacing:1.5px;color:var(--t3);text-transform:uppercase;}
.col-hd span:not(:first-child){text-align:right;}
.c4{grid-template-columns:2fr 1.1fr .8fr .8fr;}
.c5m{grid-template-columns:.55fr 1.2fr .85fr .65fr .65fr .75fr;}
.c5m span:nth-child(2){text-align:left!important;}

/* DATA ROWS */
.dr{display:grid;padding:9px 14px;border-bottom:1px solid var(--b0);align-items:center;transition:background .15s;cursor:default;}
.dr:last-child{border-bottom:none;}
.dr:hover{background:rgba(56,182,255,.03);}
.dr.c4{grid-template-columns:2fr 1.1fr .8fr .8fr;}
.dr.c5m{grid-template-columns:.55fr 1.2fr .85fr .65fr .65fr .75fr;}
.dr-asset{display:flex;align-items:center;gap:7px;}
.dr-flag{font-size:15px;line-height:1;}
.dr-tk{font-size:8.5px;font-weight:700;color:var(--ac);background:var(--ac-d);border-radius:3px;padding:1px 5px;white-space:nowrap;letter-spacing:.5px;}
.dr-nm{font-size:11px;color:var(--t1);font-weight:400;}
.dr-nms{font-size:11px;color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.dr-pr{font-family:var(--mono);font-size:12px;font-weight:600;color:var(--t1);text-align:right;font-variant-numeric:tabular-nums;}
.dr-vol{font-family:var(--mono);font-size:10px;font-weight:500;color:var(--t2);text-align:right;}
.vc{font-family:var(--mono);font-size:11px;font-weight:700;text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums;}
.vc.up{color:var(--up);}.vc.dn{color:var(--dn);}.vc.neu{color:var(--t2);}

/* STATES */
.state-box{padding:28px;text-align:center;color:var(--t3);font-size:11px;}
.spinner{display:inline-block;width:12px;height:12px;border:2px solid var(--b1);border-top-color:var(--ac);border-radius:50%;animation:spin .6s linear infinite;vertical-align:middle;margin-right:6px;}

/* FOOTER */
footer{background:var(--surface);border-top:1px solid var(--b0);padding:.75rem 1.5rem;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;font-size:9px;color:var(--t3);font-weight:500;margin-top:.5rem;position:relative;z-index:1;}
footer b{color:var(--t2);}
.footer-apis{display:flex;gap:6px;}
.fa-tag{padding:2px 7px;border-radius:3px;border:1px solid var(--b1);font-size:8px;font-weight:600;color:var(--t3);}

/* ANIMATIONS */
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes flash-up{0%{background:rgba(0,212,122,.18)}100%{background:transparent}}
@keyframes flash-dn{0%{background:rgba(255,61,90,.18)}100%{background:transparent}}
.flash-up{animation:flash-up 1s ease-out;}
.flash-dn{animation:flash-dn 1s ease-out;}

/* RESPONSIVE */
@media(max-width:960px){.g2{grid-template-columns:1fr;}}
@media(max-width:640px){
  header,main,.ibov-hero,.sbar{padding-left:.75rem;padding-right:.75rem;}
  .hd-dt,.ibov-right{display:none;}
  .ibov-price{font-size:30px;}
  .dr.c4 .vc:nth-last-child(2),.col-hd.c4 span:nth-last-child(2){display:none;}
}
</style>
</head>
<body>

<header>
  <div class="hd-left">
    <div class="logo">
      <div class="logo-icon">
        <svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M 33.1,10.8 A 16,16 0 1,0 13.2,34.5" stroke="#1471A0" stroke-width="5.5" stroke-linecap="round" fill="none"/>
          <path d="M 13.2,34.5 A 16,16 0 0,0 33.1,29.2" stroke="#1e4a72" stroke-width="5.5" stroke-linecap="round" fill="none"/>
        </svg>
      </div>
      <div>
        <div class="logo-name">Criteria</div>
        <div class="logo-sub">Financial Group</div>
      </div>
    </div>
    <div class="hd-sep"></div>
    <div>
      <div class="hd-ttl">Terminal de Mercado</div>
      <div class="hd-dt" id="hd-date"></div>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:16px">
    <div class="mkt-chip closed" id="mkt-chip"><div class="mdot"></div><span id="mkt-lbl">Fechado</span></div>
    <div class="hd-clock">
      <div class="clk-time" id="clk">--:--:--</div>
      <div class="clk-tz">Brasilia · UTC-3</div>
    </div>
  </div>
</header>

<div class="sbar">
  <div class="sbar-dots">
    <div class="si"><div class="sd" id="s-ibov"></div><span>Ibovespa</span></div>
    <div class="si"><div class="sd" id="s-acoes"></div><span>Acoes B3</span></div>
    <div class="si"><div class="sd" id="s-idx"></div><span>Indices</span></div>
    <div class="si"><div class="sd" id="s-fx"></div><span>Cambio</span></div>
    <div class="si"><div class="sd" id="s-fut"></div><span>Futuros</span></div>
    <div class="si"><div class="sd" id="s-commo"></div><span>Commodities</span></div>
    <div class="si"><div class="sd" id="s-juros"></div><span>Juros</span></div>
    <div class="si"><div class="sd" id="s-cripto"></div><span>Cripto</span></div>
    <div class="si"><div class="sd" id="s-ws" title="Snapshot offline"></div><span>WS Binance</span></div>
  </div>
  <div class="sbar-right">Atualizado: <b id="lst-upd">--:--:--</b> &nbsp;·&nbsp; Proxima: <b id="nxt-upd">--:--:--</b></div>
</div>

<div class="ibov-hero">
  <div class="ibov-inner">
    <div class="ibov-main">
      <div class="ibov-label" id="ibov-lbl">&#127463;&#127479; IBOVESPA</div>
      <div class="ibov-price" id="ibov-price">— pts</div>
      <div class="ibov-badge neu" id="ibov-badge">—</div>
    </div>
    <div class="ibov-stats">
      <div class="ibov-stat ibov-vol">
        <div class="ibov-stat-l">Neg&#243;cios</div>
        <div class="ibov-stat-v" id="ibov-vol">—</div>
      </div>
      <div class="ibov-stat">
        <div class="ibov-stat-l">Minima</div>
        <div class="ibov-stat-v" id="ibov-low">—</div>
      </div>
      <div class="ibov-stat">
        <div class="ibov-stat-l">Maxima</div>
        <div class="ibov-stat-v" id="ibov-high">—</div>
      </div>
      <div class="ibov-stat">
        <div class="ibov-stat-l">Abertura</div>
        <div class="ibov-stat-v" id="ibov-open">—</div>
      </div>
      <div class="ibov-stat">
        <div class="ibov-stat-l">Fech. Ant.</div>
        <div class="ibov-stat-v" id="ibov-prev">—</div>
      </div>
    </div>
    <div class="ibov-right">
      <div class="ibov-upd">Atualizado as <span id="ibov-ts">—</span></div>
      <button class="refresh-btn" onclick="refreshAll()">&#8635; Atualizar</button>
    </div>
  </div>
</div>

<main>
  <div class="sec-hd"><div class="sec-title">Destaques do Dia — B3</div><div class="sec-ts" id="ts-acoes"></div></div>
  <div class="g2" style="margin-bottom:.75rem">
    <div class="card">
      <div class="mover-hd up">&#9650; Maiores Altas</div>
      <div class="col-hd c5m"><span>Ticker</span><span>Empresa</span><span>Preco</span><span>Var 1h</span><span>Var Dia</span><span>Volume</span></div>
      <div id="altas"><div class="state-box"><span class="spinner"></span>Carregando...</div></div>
    </div>
    <div class="card">
      <div class="mover-hd dn">&#9660; Maiores Baixas</div>
      <div class="col-hd c5m"><span>Ticker</span><span>Empresa</span><span>Preco</span><span>Var 1h</span><span>Var Dia</span><span>Volume</span></div>
      <div id="baixas"><div class="state-box"><span class="spinner"></span>Carregando...</div></div>
    </div>
  </div>

  <div class="sec-hd"><div class="sec-title">Mercados Globais &amp; Futuros</div><div class="sec-ts" id="ts-idx"></div></div>
  <div class="g2" style="margin-bottom:.75rem">
    <div class="card">
      <div class="card-hd"><span class="card-icon">&#128202;</span><span class="card-title">Indices Mundiais</span><span class="card-badge ld" id="st-idx">...</span></div>
      <div class="col-hd c4"><span>Indice</span><span>Pontos</span><span>Var 1h</span><span>Var Dia</span></div>
      <div id="indices"><div class="state-box"><span class="spinner"></span>Carregando...</div></div>
    </div>
    <div class="card">
      <div class="card-hd"><span class="card-icon">&#9889;</span><span class="card-title">Contratos Futuros</span><span class="card-badge ld" id="st-fut">...</span></div>
      <div class="col-hd c4"><span>Contrato</span><span>Cotacao</span><span>Var 1h</span><span>Var Dia</span></div>
      <div id="futuros"><div class="state-box"><span class="spinner"></span>Carregando...</div></div>
    </div>
  </div>

  <div class="sec-hd"><div class="sec-title">Taxas de Juros</div><div class="sec-ts" id="ts-juros"></div></div>
  <div class="g1">
    <div class="card">
      <div class="card-hd"><span class="card-icon">&#128200;</span><span class="card-title">Curva de Juros</span><span class="card-badge ld" id="st-juros">...</span></div>
      <div class="col-hd c4"><span>Instrumento</span><span>Taxa</span><span>Var 1h</span><span>Var Dia</span></div>
      <div id="juros"><div class="state-box"><span class="spinner"></span>Carregando...</div></div>
    </div>
  </div>

  <div class="sec-hd"><div class="sec-title">Commodities</div><div class="sec-ts" id="ts-commo"></div></div>
  <div class="g1">
    <div class="card">
      <div class="card-hd"><span class="card-icon">&#128754;</span><span class="card-title">Commodities Globais</span><span class="card-badge ld" id="st-commo">...</span></div>
      <div class="col-hd c4"><span>Commodity</span><span>Preco</span><span>Var 1h</span><span>Var 24h</span></div>
      <div id="commo"><div class="state-box"><span class="spinner"></span>Carregando...</div></div>
    </div>
  </div>

  <div class="sec-hd"><div class="sec-title">Cambio &amp; Criptomoedas</div><div class="sec-ts" id="ts-fx"></div></div>
  <div class="g2">
    <div class="card">
      <div class="card-hd"><span class="card-icon">&#128177;</span><span class="card-title">Cambio</span><span class="card-badge ld" id="st-fx">...</span></div>
      <div class="col-hd c4"><span>Par</span><span>Cotacao (BRL)</span><span>Var 1h</span><span>Var Dia</span></div>
      <div id="moedas"><div class="state-box"><span class="spinner"></span>Carregando...</div></div>
    </div>
    <div class="card">
      <div class="card-hd"><span class="card-icon">&#8383;</span><span class="card-title">Criptomoedas</span><span class="ws-tag">SNAPSHOT</span></div>
      <div class="col-hd c4"><span>Ativo</span><span>Preco (USD)</span><span>Var 1h</span><span>Var 24h</span></div>
      <div id="cripto"><div class="state-box"><span class="spinner"></span>Carregando...</div></div>
    </div>
  </div>
</main>

<footer>
  <span>&#169; 2025 Criteria Financial Group &middot; Terminal Snapshot</span>
  <div class="footer-apis">
    <span class="fa-tag">SNAPSHOT</span>
    <span class="fa-tag">26/05/2026</span>
    <span class="fa-tag">Fechamento B3</span>
  </div>
  <span>Ultima atualizacao: <b id="fu-ts">—</b></span>
</footer>

<script>
'use strict';

/* ── Price history (localStorage) ─────────────────────────── */
const PH={
  k:key=>'ph8_'+key,
  push(key,price){if(price==null||isNaN(+price))return;try{const k=this.k(key),now=Date.now();const arr=JSON.parse(localStorage.getItem(k)||'[]');arr.push({t:now,p:+price});localStorage.setItem(k,JSON.stringify(arr.filter(x=>now-x.t<14_400_000).slice(-200)));}catch{}},
  calc1h(key,cur){if(cur==null||isNaN(+cur))return null;try{const arr=JSON.parse(localStorage.getItem(this.k(key))||'[]');if(arr.length<2)return null;const now=Date.now();const ref=arr.filter(x=>now-x.t>=3_000_000&&now-x.t<=4_500_000).slice(-1)[0]||arr.filter(x=>now-x.t>=900_000)[0]||arr.filter(x=>now-x.t>=180_000)[0]||arr[0];return(!ref||ref.p===0)?null:((+cur-ref.p)/ref.p)*100;}catch{return null;}}
};

/* ── Helpers ───────────────────────────────────────────────── */
const $=id=>document.getElementById(id);
const n=(v,d=2)=>v==null||isNaN(+v)?'—':(+v).toLocaleString('pt-BR',{minimumFractionDigits:d,maximumFractionDigits:d});
const usd=v=>{if(v==null||isNaN(+v))return'—';v=+v;if(v>=10000)return'US$ '+n(v,0);if(v>=100)return'US$ '+n(v,2);if(v>=1)return'US$ '+n(v,3);return'US$ '+v.toFixed(5);};
const brl=v=>v==null||isNaN(+v)?'—':'R$ '+n(v,4);
const volf=v=>{if(!v||isNaN(+v))return'—';v=+v;if(v>=1e12)return'R$ '+(v/1e12).toFixed(2)+' tri';if(v>=1e9)return'R$ '+(v/1e9).toFixed(2)+' bi';if(v>=1e6)return'R$ '+(v/1e6).toFixed(0)+' mi';return'R$ '+v.toLocaleString('pt-BR');};
const vols=v=>{if(!v||isNaN(+v))return'—';v=+v;if(v>=1e6)return(v/1e6).toFixed(1)+' mi';if(v>=1e3)return(v/1e3).toFixed(0)+' mil';return v.toLocaleString('pt-BR');};
const br=()=>new Date().toLocaleTimeString('pt-BR',{timeZone:'America/Sao_Paulo',hour12:false});
const set=(id,v)=>{const e=$(id);if(e)e.textContent=v;};
const secTs=(id,t)=>{const e=$(id);if(e)e.textContent=t?'atualizado '+t:'';};

function pct(v){
  if(v==null||isNaN(+v))return'<span class="vc neu">—</span>';
  const c=+v>0?'up':+v<0?'dn':'neu',a=+v>0?'▲':+v<0?'▼':'—',s=+v>0?'+':'';
  return`<span class="vc ${c}">${a} ${s}${n(v,2)}%</span>`;
}

const _prev=new Map();
function flash(el,val,key){
  if(!el||val==null)return;
  const pv=_prev.get(key);
  if(pv===undefined){_prev.set(key,val);return;}
  if(pv===val)return;
  const cls=+val>+pv?'flash-up':'flash-dn';
  _prev.set(key,val);
  el.classList.remove('flash-up','flash-dn');
  void el.offsetWidth;
  el.classList.add(cls);
  setTimeout(()=>el.classList.remove('flash-up','flash-dn'),1000);
}

function dot(id,st,tip=''){const e=$(id);if(!e)return;e.className='sd'+(st==='ok'?' ok':st==='ld'?' ld':st==='er'?' er':'');if(tip)e.title=tip;}
function badge(id,st,lbl){const e=$(id);if(!e)return;e.className='card-badge '+(st==='ok'?'ok':st==='er'?'er':st==='cl'?'cl':'ld');e.textContent=lbl;}

function getMktStatus(){
  const brn=new Date(new Date().toLocaleString('en-US',{timeZone:'America/Sao_Paulo'}));
  const dow=brn.getDay(),mins=brn.getHours()*60+brn.getMinutes();
  if(dow===0||dow===6)return{key:'closed',label:'Fechado — Fim de Semana'};
  if(mins<9*60+45)return{key:'closed',label:'Fechado'};
  if(mins<10*60)return{key:'pre',label:'Pre-Abertura'};
  if(mins<17*60+55)return{key:'open',label:'Pregao Aberto'};
  if(mins<18*60+30)return{key:'after',label:'After-Market'};
  return{key:'closed',label:'Fechado · Ultimo Fechamento'};
}
function updateChip(){const{key,label}=getMktStatus();const c=$('mkt-chip'),l=$('mkt-lbl');if(c)c.className='mkt-chip '+key;if(l)l.textContent=label;}
const isClosed=()=>getMktStatus().key==='closed';

/* ── Mock data (snapshot 26/05/2026 — fechamento B3) ───────── */
function getMockData(e){
  const D={
    ibov:{price:177816,change:.47,volume:null,trades:1192059,low:176208,high:178034,open:176510,prev:176976},
    acoes:{
      total:10,
      altas:[
        {tk:'ASAI3',nm:'Assai Atacadista',p:9.12,pct:8.06,vol:null},
        {tk:'CYRE3',nm:'Cyrela',p:22.67,pct:6.68,vol:null},
        {tk:'POMO4',nm:'Marcopolo PN',p:6.22,pct:4.89,vol:null},
        {tk:'COGN3',nm:'Cogna',p:2.56,pct:4.49,vol:null},
        {tk:'MRVE3',nm:'MRV Engenharia',p:6.27,pct:3.98,vol:null}
      ],
      baixas:[
        {tk:'PRIO3',nm:'PetroRio',p:64.31,pct:-5.98,vol:null},
        {tk:'USIM5',nm:'Usiminas PNA',p:10.02,pct:-3.19,vol:null},
        {tk:'PETR3',nm:'Petrobras ON',p:48.69,pct:-2.91,vol:null},
        {tk:'SMTO3',nm:'Sao Martinho',p:17.31,pct:-1.65,vol:null},
        {tk:'VBBR3',nm:'Vibra Energia',p:32.28,pct:-1.44,vol:null}
      ]
    },
    indices:{items:[
      {fl:'🇺🇸',tk:'SPX',nm:'S&amp;P 500',price:7473,change:.37,d:0},
      {fl:'🇺🇸',tk:'DJI',nm:'Dow Jones',price:50580,change:.58,d:0},
      {fl:'🇺🇸',tk:'NDX',nm:'Nasdaq',price:26344,change:.19,d:0},
      {fl:'🇯🇵',tk:'N225',nm:'Nikkei 225',price:64996,change:-.25,d:0},
      {fl:'🇪🇺',tk:'SX5E',nm:'Euro Stoxx 50',price:6094,change:-.70,d:0},
      {fl:'🇭🇰',tk:'HSI',nm:'Hang Seng',price:25599,change:-.03,d:0}
    ]},
    futuros:{items:[
      {fl:'🇧🇷',tk:'WIN',nm:'Indice Mini (WINM26) aprox.IBOV',price:177816,change:.91,fmt:'pts'},
      {fl:'🇺🇸',tk:'WDO',nm:'Dolar Mini (WDOM26) USD-BRL',price:5.0128,change:null,fmt:'brl'}
    ]},
    juros:{items:[
      {fl:'🇧🇷',tk:'SELIC',nm:'Selic Meta',price:14.50,change:null,d:2},
      {fl:'🇧🇷',tk:'CDI',nm:'CDI Over',price:14.40,change:null,d:2},
      {fl:'🇧🇷',tk:'IPCA',nm:'IPCA (mes)',price:.67,change:null,d:2},
      {fl:'🇺🇸',tk:'UST2',nm:'T-Note 2A',price:4.256,change:0,d:3},
      {fl:'🇺🇸',tk:'UST10',nm:'T-Note 10A',price:4.558,change:0,d:3},
      {fl:'🇺🇸',tk:'UST30',nm:'T-Bond 30A',price:5.064,change:0,d:3}
    ]},
    commodities:{items:[
      {fl:'🛢️',tk:'BRENT',nm:'Petroleo Brent',price:96.050,change:-4.15},
      {fl:'🛢️',tk:'WTI',nm:'Petroleo WTI',price:92.660,change:-4.08},
      {fl:'🥇',tk:'GOLD',nm:'Ouro',price:4508.80,change:-.32},
      {fl:'🥈',tk:'SLVR',nm:'Prata',price:76.350,change:.20},
      {fl:'🌽',tk:'CORN',nm:'Milho',price:458.50,change:-1.03},
      {fl:'🌱',tk:'SOYB',nm:'Soja',price:1190.25,change:-.52},
      {fl:'🐂',tk:'LIVE',nm:'Boi Gordo',price:239.60,change:0},
      {fl:'☕',tk:'COFF',nm:'Cafe Arabica',price:272.80,change:.17}
    ]},
    moedas:{items:[
      {fl:'🇺🇸',tk:'USD',nm:'Dolar Comercial',bid:5.0152,change:null},
      {fl:'🇪🇺',tk:'EUR',nm:'Euro',bid:5.8386,change:null},
      {fl:'🇬🇧',tk:'GBP',nm:'Libra Esterlina',bid:6.7622,change:null},
      {fl:'🇯🇵',tk:'JPY',nm:'Iene Japones',bid:.0315,change:null}
    ]},
    cripto:{items:[
      {fl:'₿',tk:'BTC',nm:'Bitcoin',price:76948,change1h:null,change24h:-.19},
      {fl:'Ξ',tk:'ETH',nm:'Ethereum',price:2113.49,change1h:null,change24h:.36},
      {fl:'◎',tk:'SOL',nm:'Solana',price:84.850,change1h:null,change24h:-.67},
      {fl:'✕',tk:'XRP',nm:'XRP',price:1.348,change1h:null,change24h:-.38},
      {fl:'◈',tk:'BNB',nm:'BNB',price:660.70,change1h:null,change24h:-.13}
    ]}
  };
  return D[e]||null;
}

/* Backend na nuvem. Troque a URL abaixo pela do SEU servico (ex: Render).
   Dica: da pra testar sem editar, abrindo terminal-mercado.html?api=https://SEU-APP.onrender.com/api/ */
const LIVE_BASE=(new URLSearchParams(location.search).get('api'))||'/api/';
async function api(endpoint){
  /* Tenta o servidor live primeiro; cai no mock se offline */
  try{
    const r=await fetch(LIVE_BASE+endpoint,{signal:AbortSignal.timeout(4000)});
    if(r.ok){const j=await r.json();if(j.ok)return j.data;}
  }catch(_){}
  /* Fallback: snapshot 26/05/2026 */
  const d=getMockData(endpoint);
  if(!d)throw new Error('sem dados para '+endpoint);
  return d;
}

/* ── Render functions ──────────────────────────────────────── */
function renderIBOV(d){
  if(!d)return;
  const cl=isClosed();
  const el=$('ibov-price');
  if(el){flash(el,d.price,'ibov');el.textContent=(d.price?n(d.price,0):'—')+' pts';}
  const b=$('ibov-badge');
  if(b){b.className='ibov-badge '+(d.change==null?'neu':d.change>=0?'up':'dn');b.textContent=d.change==null?'—':(d.change>=0?'▲ +':'▼ ')+n(d.change,2)+'%';}
  const lbl=$('ibov-lbl');
  if(lbl)lbl.textContent=cl?'🇧🇷 IBOVESPA · FECHAMENTO':'🇧🇷 IBOVESPA';
  set('ibov-vol', d.trades ? (+d.trades).toLocaleString('pt-BR') : (d.volume ? volf(d.volume) : '—'));
  set('ibov-low',d.low?n(d.low,0):'—');
  set('ibov-high',d.high?n(d.high,0):'—');
  set('ibov-open',d.open?n(d.open,0):'—');
  set('ibov-prev',d.prev?n(d.prev,0):'—');
  set('ibov-ts',cl?'fechamento':'agora · '+br());
  PH.push('ibov',d.price);
}
async function loadIBOV(){dot('s-ibov','ld');try{const d=await api('ibov');renderIBOV(d);dot('s-ibov','ok','Ibovespa OK');}catch(e){dot('s-ibov','er','Erro: '+e.message);}}

function rowMover(tk,nm,p,v1h,vd,vol){return`<div class="dr c5m"><div><span class="dr-tk">${tk}</span></div><div class="dr-nms" title="${nm}">${nm}</div><div class="dr-pr">R$ ${n(p,2)}</div>${pct(v1h)}${pct(vd)}<div class="dr-vol">${vols(vol)}</div></div>`;}
function renderMovers(d){
  $('altas').innerHTML=d.altas&&d.altas.length?d.altas.map(x=>rowMover(x.tk,x.nm,x.p,PH.calc1h('stk_'+x.tk,x.p),x.pct,x.vol)).join(''):'<div class="state-box">Sem dados</div>';
  $('baixas').innerHTML=d.baixas&&d.baixas.length?d.baixas.map(x=>rowMover(x.tk,x.nm,x.p,PH.calc1h('stk_'+x.tk,x.p),x.pct,x.vol)).join(''):'<div class="state-box">Sem dados</div>';
  if(d.altas)d.altas.forEach(x=>PH.push('stk_'+x.tk,x.p));
  if(d.baixas)d.baixas.forEach(x=>PH.push('stk_'+x.tk,x.p));
}
async function loadAcoes(){dot('s-acoes','ld');try{const d=await api('acoes');renderMovers(d);secTs('ts-acoes',isClosed()?'fechamento':br());dot('s-acoes','ok','Acoes: '+d.total);}catch(e){dot('s-acoes','er','Erro: '+e.message);}}

function row4(fl,tk,nm,pr,v1h,vd,key){return`<div class="dr c4" id="row-${key}"><div class="dr-asset"><span class="dr-flag">${fl}</span><span class="dr-tk">${tk}</span><span class="dr-nm">${nm}</span></div><div class="dr-pr" id="pr-${key}">${pr}</div>${pct(v1h)}${pct(vd)}</div>`;}

function renderIndices(d){
  $('indices').innerHTML=d.items.map(x=>{PH.push('idx_'+x.tk,x.price);return row4(x.fl,x.tk,x.nm,x.price!=null?n(x.price,x.d):'—',PH.calc1h('idx_'+x.tk,x.price),x.change,'idx_'+x.tk);}).join('');
  const cnt=d.items.filter(x=>x.price).length;
  const cl=isClosed();
  badge('st-idx',cnt>0?(cl?'cl':'ok'):'er',cnt>0?(cl?'FECH.':cnt+'/'+d.items.length):'Erro');
}
async function loadIndices(){dot('s-idx','ld');badge('st-idx','ld','...');try{const d=await api('indices');renderIndices(d);secTs('ts-idx',br());dot('s-idx','ok');}catch(e){dot('s-idx','er');badge('st-idx','er','Erro');}}

function renderFuturos(d){
  const cl=isClosed();
  const cnt=d.items.filter(x=>x.price).length;
  badge('st-fut',cnt>0?(cl?'cl':'ok'):'er',cnt>0?(cl?'FECH.':cnt+'/2'):'Erro');
  $('futuros').innerHTML=d.items.map(x=>{
    PH.push('fut_'+x.tk,x.price);
    let pr='—';
    if(x.price)pr=x.fmt==='pts'?n(x.price,0)+' pts':'R$ '+n(x.price,4);
    return row4(x.fl,x.tk,x.nm,pr,PH.calc1h('fut_'+x.tk,x.price),x.change,'fut_'+x.tk);
  }).join('');
}
async function loadFuturos(){dot('s-fut','ld');badge('st-fut','ld','...');try{const d=await api('futuros');renderFuturos(d);dot('s-fut','ok');}catch(e){dot('s-fut','er');badge('st-fut','er','Erro');}}

function renderJuros(d){
  const cl=isClosed();
  const cnt=d.items.filter(x=>x.price).length;
  badge('st-juros',cnt>0?(cl?'cl':'ok'):'er',cnt>0?(cl?'FECH.':cnt+'/'+d.items.length):'Erro');
  $('juros').innerHTML=d.items.map(x=>{PH.push('jr_'+x.tk,x.price);return row4(x.fl,x.tk,x.nm,x.price!=null?n(x.price,x.d)+'%':'—',PH.calc1h('jr_'+x.tk,x.price),x.change,'jr_'+x.tk);}).join('');
}
async function loadJuros(){dot('s-juros','ld');badge('st-juros','ld','...');try{const d=await api('juros');renderJuros(d);secTs('ts-juros',br());dot('s-juros','ok');}catch(e){dot('s-juros','er');badge('st-juros','er','Erro');}}

function renderCommo(d){
  const cl=isClosed();
  const cnt=d.items.filter(x=>x.price).length;
  badge('st-commo',cnt>0?(cl?'cl':'ok'):'er',cnt>0?(cl?'FECH.':cnt+'/'+d.items.length):'Erro');
  $('commo').innerHTML=d.items.map(x=>{PH.push('co_'+x.tk,x.price);return row4(x.fl,x.tk,x.nm,x.price!=null?usd(x.price):'—',PH.calc1h('co_'+x.tk,x.price),x.change,'co_'+x.tk);}).join('');
}
async function loadCommo(){dot('s-commo','ld');badge('st-commo','ld','...');try{const d=await api('commodities');renderCommo(d);secTs('ts-commo',br());dot('s-commo','ok');}catch(e){dot('s-commo','er');badge('st-commo','er','Erro');}}

function renderMoedas(d){
  const cl=isClosed();
  const cnt=d.items.filter(x=>x.bid).length;
  badge('st-fx',cnt>0?(cl?'cl':'ok'):'er',cnt>0?(cl?'FECH.':cnt+'/'+d.items.length):'Erro');
  $('moedas').innerHTML=d.items.map(x=>{PH.push('fx_'+x.tk,x.bid);return row4(x.fl,x.tk,x.nm,x.bid!=null?brl(x.bid):'—',PH.calc1h('fx_'+x.tk,x.bid),x.change,'fx_'+x.tk);}).join('');
}
async function loadMoedas(){dot('s-fx','ld');badge('st-fx','ld','...');try{const d=await api('moedas');renderMoedas(d);secTs('ts-fx',br());dot('s-fx','ok');}catch(e){dot('s-fx','er');badge('st-fx','er','Erro');}}

const criptoCache={};
function renderCripto(d){
  if(d&&d.items)d.items.forEach(c=>{criptoCache[c.nm]=c;});
  const vals=Object.values(criptoCache);
  $('cripto').innerHTML=vals.length?vals.map(c=>`<div class="dr c4"><div class="dr-asset"><span class="dr-flag">${c.fl}</span><span class="dr-tk">${c.tk}</span><span class="dr-nm">${c.nm}</span></div><div class="dr-pr" id="cp-${c.nm}">${c.price!=null?usd(c.price):'—'}</div>${pct(c.change1h)}${pct(c.change24h)}</div>`).join(''):'<div class="state-box">Sem dados</div>';
}
async function loadCripto(){dot('s-cripto','ld');try{const d=await api('cripto');renderCripto(d);dot('s-cripto','ok','Snapshot OK');}catch(e){dot('s-cripto','er','Erro: '+e.message);}}

/* WS desabilitado no modo standalone */
function connectWS(){dot('s-ws','','Snapshot offline');}

/* ── Clock & scheduler ─────────────────────────────────────── */
function tick(){
  const now=new Date();
  set('clk',now.toLocaleTimeString('pt-BR',{timeZone:'America/Sao_Paulo',hour12:false}));
  const d=now.toLocaleDateString('pt-BR',{timeZone:'America/Sao_Paulo',weekday:'long',day:'2-digit',month:'long',year:'numeric'});
  set('hd-date',d.charAt(0).toUpperCase()+d.slice(1));
  updateChip();
}
setInterval(tick,1000);tick();

function setTs(){
  const t=br();
  set('lst-upd',t);
  set('fu-ts',t);
  set('nxt-upd',new Date(Date.now()+45_000).toLocaleTimeString('pt-BR',{timeZone:'America/Sao_Paulo'}));
}

async function loadAll(){
  await Promise.allSettled([loadIBOV(),loadAcoes(),loadIndices(),loadMoedas(),loadFuturos(),loadCommo(),loadJuros(),loadCripto()]);
  setTs();
}
function refreshAll(){set('ibov-ts','atualizando...');loadAll();}

document.addEventListener('visibilitychange',()=>{if(!document.hidden)loadAll();});
window.addEventListener('online',()=>loadAll());

loadAll();
connectWS();
</script>
</body>
</html>"""


if __name__ == "__main__":
    # execucao local opcional: python app.py
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)
