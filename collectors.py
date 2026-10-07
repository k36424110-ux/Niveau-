"""Ingestion multi-exchange : trades (WS), carnets (REST), liquidations, funding/OI, bougies."""
import asyncio, json, logging, time
from collections import deque
import httpx, websockets

log = logging.getLogger("collectors")


async def ws_loop(name, url, on_msg, sub=None):
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**23) as ws:
                if sub:
                    await ws.send(json.dumps(sub))
                log.info("%s connecté", name)
                async for raw in ws:
                    try:
                        on_msg(json.loads(raw))
                    except Exception as e:
                        log.debug("%s parse: %s", name, e)
        except Exception as e:
            log.warning("%s déconnecté: %s", name, e)
        await asyncio.sleep(3)


async def poll(name, fn, every):
    while True:
        try:
            await fn()
        except Exception as e:
            log.warning("%s poll: %s", name, e)
        await asyncio.sleep(every)


def start_all(eng):
    cl = httpx.AsyncClient(timeout=10)
    f = lambda rows: [(float(p), float(q)) for p, q, *_ in rows]

    # ---- Binance spot + perp : trades agressifs + liquidations
    def binance(ex):
        def h(m):
            d = m.get("data", m)
            e = d.get("e")
            if e == "aggTrade":
                eng.add_trade(ex, d["T"] / 1000, float(d["p"]), float(d["q"]), "sell" if d["m"] else "buy")
            elif e == "forceOrder":
                o = d["o"]
                eng.add_liq(o["T"] / 1000, float(o["ap"] or o["p"]), float(o["ap"] or o["p"]) * float(o["q"]), o["S"] == "SELL")
        return h

    # ---- Coinbase (référence institutionnelle spot)
    def coinbase(m):
        if m.get("type") in ("match", "last_match"):
            t = time.time()
            # side = côté du maker -> l'agresseur est l'inverse
            eng.add_trade("coinbase", t, float(m["price"]), float(m["size"]), "buy" if m["side"] == "sell" else "sell")

    # ---- Bybit perp : trades + liquidations
    def bybit(m):
        tp = m.get("topic", "")
        if tp.startswith("publicTrade"):
            for x in m["data"]:
                eng.add_trade("bybit", x["T"] / 1000, float(x["p"]), float(x["v"]), x["S"].lower())
        elif tp.startswith("allLiquidation"):
            for x in m["data"]:
                p, q = float(x["p"]), float(x["v"])
                eng.add_liq(x["T"] / 1000, p, p * q, x["S"] == "Buy")

    async def book(ex, url, parse):
        r = (await cl.get(url)).json()
        b, a = parse(r)
        eng.set_book(ex, f(b), f(a))

    async def candles(tf, gran):
        r = (await cl.get(f"https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity={gran}",
                          headers={"User-Agent": "btc-zones"})).json()
        rows = sorted(r, key=lambda x: x[0])
        eng.candles[tf] = [dict(t=x[0], l=x[1], h=x[2], o=x[3], c=x[4]) for x in rows]

    oi_hist = deque(maxlen=200)

    async def deriv():
        pi = (await cl.get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT")).json()
        oi = float((await cl.get("https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT")).json()["openInterest"])
        now = time.time()
        oi_hist.append((now, oi))
        old = next((v for t, v in oi_hist if now - t <= 900), oi)
        eng.ctx.update(funding=float(pi["lastFundingRate"]), oi=oi, oi_chg_15m=(oi - old) / old * 100 if old else 0)

    B = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
    P = "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
    C = "https://api.exchange.coinbase.com/products/BTC-USD/book?level=2"
    Y = "https://api.bybit.com/v5/market/orderbook?category=linear&symbol=BTCUSDT&limit=200"
    T = []
    T += [ws_loop("binance", "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade", binance("binance"))]
    T += [ws_loop("binance_perp", "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@forceOrder", binance("binance_perp"))]
    T += [ws_loop("coinbase", "wss://ws-feed.exchange.coinbase.com", coinbase,
                  {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches"]})]
    T += [ws_loop("bybit", "wss://stream.bybit.com/v5/public/linear", bybit,
                  {"op": "subscribe", "args": ["publicTrade.BTCUSDT", "allLiquidation.BTCUSDT"]})]
    T += [poll("book_binance", lambda: book("binance", B, lambda r: (r["bids"], r["asks"])), 3)]
    T += [poll("book_perp", lambda: book("binance_perp", P, lambda r: (r["bids"], r["asks"])), 3)]
    T += [poll("book_coinbase", lambda: book("coinbase", C, lambda r: (r["bids"], r["asks"])), 3)]
    T += [poll("book_bybit", lambda: book("bybit", Y, lambda r: (r["result"]["b"], r["result"]["a"])), 3)]
    T += [poll("c15", lambda: candles("15m", 900), 60), poll("c1h", lambda: candles("1h", 3600), 120)]
    T += [poll("deriv", deriv, 30)]
    return [asyncio.create_task(t) for t in T]
