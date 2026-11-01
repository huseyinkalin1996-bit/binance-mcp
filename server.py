"""Binance USDT-M Futures tarayici MCP sunucusu (sadece herkese acik veri, API anahtari yok)."""
import asyncio
import os

import httpx
from mcp.server.fastmcp import FastMCP

BASE = os.environ.get("BINANCE_FAPI", "https://fapi.binance.com")
PORT = int(os.environ.get("PORT", "8000"))
PATH = os.environ.get("MCP_PATH", "/mcp")
INTERVALS = {"1m", "3m", "5m", "15m", "30m", "1h", "4h"}

kwargs = dict(host="0.0.0.0", port=PORT, streamable_http_path=PATH, stateless_http=True)
try:
    from mcp.server.transport_security import TransportSecuritySettings

    kwargs["transport_security"] = TransportSecuritySettings(
        enable_dns_rebinding_protection=False
    )
except Exception:
    pass

mcp = FastMCP("binance-futures-scanner", **kwargs)


def ema(vals, n):
    k = 2 / (n + 1)
    out, e = [], vals[0]
    for v in vals:
        e = v * k + e * (1 - k)
        out.append(e)
    return out


def rsi(closes, n=14):
    if len(closes) <= n:
        return None
    g = l = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        g += max(d, 0)
        l += max(-d, 0)
    ag, al = g / n, l / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def vwap(h, l, c, v, n=14):
    tp = [(a + b + d) / 3 for a, b, d in zip(h, l, c)]
    vs = sum(v[-n:])
    return sum(t * x for t, x in zip(tp[-n:], v[-n:])) / vs if vs else tp[-1]


def atr_pct(h, l, c, n=14):
    trs = [
        max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
        for i in range(1, len(c))
    ]
    a = sum(trs[-n:]) / min(n, len(trs))
    return a / c[-1] * 100


def analyze(symbol, rows):
    if len(rows) < 40:
        return None
    live = float(rows[-1][4])
    cl = rows[:-1]
    h = [float(r[2]) for r in cl]
    l = [float(r[3]) for r in cl]
    c = [float(r[4]) for r in cl]
    v = [float(r[5]) for r in cl]
    e9, e21 = ema(c, 9)[-1], ema(c, 21)[-1]
    vw = vwap(h, l, c, v)
    rs = rsi(c)
    atr = atr_pct(h, l, c)
    avgv = sum(v[-11:-1]) / 10
    relvol = v[-1] / avgv if avgv else 0.0
    dist9 = (live - e9) / e9 * 100
    ema_gap = abs(e9 - e21) / live * 100

    up = live > e9 > e21 and live > vw
    down = live < e9 < e21 and live < vw
    if (not up and not down) or ema_gap < 0.3 * atr:
        tag = "CHOP"
    else:
        side = "UP" if up else "DOWN"
        if abs(dist9) > 2 * atr:
            tag = f"EXTENDED_{side}"
        elif abs(dist9) <= 0.8 * atr and relvol >= 1:
            tag = f"PULLBACK_{side}"
        else:
            tag = f"TREND_{side}"

    score = relvol
    if tag.startswith("PULLBACK"):
        score += 2
    elif tag.startswith("TREND"):
        score += 0.5
    elif tag == "CHOP":
        score -= 3
    return {
        "symbol": symbol,
        "price": live,
        "tag": tag,
        "score": round(score, 2),
        "rel_volume": round(relvol, 2),
        "rsi": round(rs, 1) if rs is not None else None,
        "dist_ema9_pct": round(dist9, 2),
        "atr_pct": round(atr, 2),
        "ema9": e9,
        "ema21": e21,
        "vwap14": vw,
    }


async def _get(client, path, params=None):
    r = await client.get(BASE + path, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def _err(e):
    if isinstance(e, httpx.HTTPStatusError):
        code = e.response.status_code
        hint = ""
        if code in (451, 403):
            hint = " Binance bu sunucunun bolgesini engelliyor olabilir; hosting bolgesini Avrupa/Singapur yap."
        return {"error": f"Binance HTTP {code}.{hint}"}
    return {"error": f"{type(e).__name__}: {e}"}


@mcp.tool()
async def scan_movers(
    min_quote_volume_usdt: float = 100_000_000,
    max_symbols: int = 40,
    interval: str = "5m",
) -> dict:
    """24s USDT hacmi yuksek Binance perp coinlerini tarar, EMA9/EMA21/VWAP14/RSI/rel. hacme gore
    etiketler (PULLBACK_*, TREND_*, EXTENDED_*, CHOP) ve skora gore siralar."""
    if interval not in INTERVALS:
        return {"error": f"interval su olmali: {sorted(INTERVALS)}"}
    max_symbols = max(1, min(max_symbols, 80))
    try:
        async with httpx.AsyncClient() as client:
            tickers = await _get(client, "/fapi/v1/ticker/24hr")
            pool = [
                t
                for t in tickers
                if t["symbol"].endswith("USDT")
                and "_" not in t["symbol"]
                and float(t["quoteVolume"]) >= min_quote_volume_usdt
            ]
            pool.sort(key=lambda t: float(t["quoteVolume"]), reverse=True)
            pool = pool[:max_symbols]
            sem = asyncio.Semaphore(8)

            async def one(t):
                async with sem:
                    try:
                        rows = await _get(
                            client,
                            "/fapi/v1/klines",
                            {"symbol": t["symbol"], "interval": interval, "limit": 100},
                        )
                    except Exception:
                        return None
                a = analyze(t["symbol"], rows)
                if a:
                    a["change_24h_pct"] = round(float(t["priceChangePercent"]), 2)
                    a["vol_24h_usdt_m"] = round(float(t["quoteVolume"]) / 1e6, 1)
                return a

            res = [r for r in await asyncio.gather(*[one(t) for t in pool]) if r]
    except Exception as e:
        return _err(e)
    res.sort(key=lambda r: r["score"], reverse=True)
    return {"interval": interval, "scanned": len(res), "results": res}


@mcp.tool()
async def get_analysis(symbol: str, interval: str = "5m") -> dict:
    """Tek coin icin detay: gostergeler, son 12 kapanmis mum, swing tepe/dip, funding rate."""
    if interval not in INTERVALS:
        return {"error": f"interval su olmali: {sorted(INTERVALS)}"}
    symbol = symbol.upper().strip()
    try:
        async with httpx.AsyncClient() as client:
            rows, prem = await asyncio.gather(
                _get(
                    client,
                    "/fapi/v1/klines",
                    {"symbol": symbol, "interval": interval, "limit": 100},
                ),
                _get(client, "/fapi/v1/premiumIndex", {"symbol": symbol}),
            )
    except Exception as e:
        return _err(e)
    a = analyze(symbol, rows)
    if not a:
        return {"error": "Yetersiz mum verisi."}
    closed = rows[:-1]
    last = [
        {
            "t": r[0],
            "o": float(r[1]),
            "h": float(r[2]),
            "l": float(r[3]),
            "c": float(r[4]),
            "vol": float(r[5]),
        }
        for r in closed[-12:]
    ]
    recent = closed[-60:]
    a["swing_high_60"] = max(float(r[2]) for r in recent)
    a["swing_low_60"] = min(float(r[3]) for r in recent)
    a["mark_price"] = float(prem["markPrice"])
    a["funding_rate_pct"] = round(float(prem["lastFundingRate"]) * 100, 4)
    a["last_closed_candles"] = last
    return a


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
