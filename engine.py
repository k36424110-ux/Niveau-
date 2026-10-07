"""Moteur : transforme trades/carnets/liquidations/bougies en zones scorées par confluence."""
import os, time
from collections import defaultdict, deque

PB, WB = 50, 25                                   # buckets profil / murs ($)
LARGE_USD = float(os.getenv("LARGE_USD", "250000"))
WALL_MIN = float(os.getenv("WALL_MIN_BTC", "20"))
LIQ_MIN = float(os.getenv("LIQ_MIN_USD", "500000"))
MAX_RAW = 13.0                                    # confluence max théorique -> score 100


class Engine:
    def __init__(s):
        s.last, s.books, s.candles, s.ctx = {}, {}, {}, {}
        s.bars = {}                                              # minute -> [o,h,l,c,vol,delta]
        s.prof = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))
        s.exd = defaultdict(lambda: defaultdict(float))          # delta par exchange
        s.big, s.liq = deque(maxlen=5000), deque(maxlen=5000)
        s.wall_seen, s.pulled = {}, 0
        s.snapshot = {"ready": False}

    # ------------------------------------------------------------ ingestion
    def add_trade(s, ex, ts, p, q, side):
        s.last[ex] = (ts, p)
        m, sg = int(ts // 60), (1 if side == "buy" else -1)
        b = s.bars.setdefault(m, [None, -1e18, 1e18, None, 0.0, 0.0])
        ref = "binance" if time.time() - s.last.get("binance", (0,))[0] < 20 else "coinbase"
        if ex == ref:
            b[0] = p if b[0] is None else b[0]
            b[1], b[2], b[3] = max(b[1], p), min(b[2], p), p
        b[4] += q
        b[5] += sg * q
        s.prof[m][int(p // PB) * PB][0 if sg > 0 else 1] += q
        s.exd[ex][m] += sg * q
        if p * q >= LARGE_USD:
            s.big.append((ts, p, p * q, sg))
        if len(s.bars) > 400:
            lim = m - 300
            for d in (s.bars, s.prof, *s.exd.values()):
                for k in [k for k in d if k < lim]:
                    del d[k]

    def add_liq(s, ts, p, usd, long_liq):
        s.liq.append((ts, p, usd, long_liq))

    def set_book(s, ex, bids, asks):
        s.books[ex] = (time.time(), bids, asks)

    # ------------------------------------------------------------ murs (anti-spoof)
    def _walls(s, mid, now):
        res = []
        for side in ("bid", "ask"):
            agg = defaultdict(float)
            for t, bids, asks in s.books.values():
                if now - t > 20:
                    continue
                for p, q in (bids if side == "bid" else asks):
                    if abs(p - mid) < mid * 0.015:
                        agg[int(p // WB) * WB] += q
            cur = set()
            if len(agg) >= 10:
                v = list(agg.values())
                mu = sum(v) / len(v)
                sd = (sum((x - mu) ** 2 for x in v) / len(v)) ** 0.5
                thr = max(mu + 2.5 * sd, WALL_MIN)
                cur = {k for k, x in agg.items() if x >= thr}
            for k in cur:
                s.wall_seen.setdefault((side, k), now)
            for key in [k for k in s.wall_seen if k[0] == side and k[1] not in cur]:
                if now - s.wall_seen.pop(key) < 120:
                    s.pulled += 1                      # mur retiré vite = spoof probable
            for k in cur:
                age = now - s.wall_seen[(side, k)]
                if age >= 15:
                    res.append((side, k, agg[k], age))
        return res

    # ------------------------------------------------------------ structure ICT/SMC
    def _structure(s, rows, tag, wob, price, add):
        n = len(rows)
        if n < 60:
            return
        atr = sum(r["h"] - r["l"] for r in rows[-14:]) / 14
        for i in range(max(2, n - 60), n - 2):
            p, a, b = rows[i - 1], rows[i], rows[i + 1]
            nxt = rows[i + 2:]
            if a["c"] < a["o"] and b["c"] - b["o"] >= 1.5 * atr and b["c"] > a["h"] and all(r["l"] > a["l"] for r in nxt):
                add(a["l"], a["h"], "ob", f"OB haussier {tag}", wob, "long")
            if a["c"] > a["o"] and b["o"] - b["c"] >= 1.5 * atr and b["c"] < a["l"] and all(r["h"] < a["h"] for r in nxt):
                add(a["l"], a["h"], "ob", f"OB baissier {tag}", wob, "short")
            if b["l"] - p["h"] >= 0.3 * atr and all(r["l"] > p["h"] for r in nxt):
                add(p["h"], b["l"], "fvg", f"FVG haussier {tag}", 1.2, "long")
            if p["l"] - b["h"] >= 0.3 * atr and all(r["h"] < p["l"] for r in nxt):
                add(b["h"], p["l"], "fvg", f"FVG baissier {tag}", 1.2, "short")
        # equal highs / lows non balayés = pools de liquidité
        for key, up in (("h", True), ("l", False)):
            piv = [(i, rows[i][key]) for i in range(max(3, n - 150), n - 3)
                   if rows[i][key] == (max if up else min)(r[key] for r in rows[i - 3:i + 4])]
            for x in range(len(piv)):
                for y in range(x + 1, len(piv)):
                    (_, v1), (j, v2) = piv[x], piv[y]
                    lvl = max(v1, v2) if up else min(v1, v2)
                    if abs(v1 - v2) / lvl > 0.0006:
                        continue
                    if up and lvl > price and all(r["h"] < lvl * 1.001 for r in rows[j + 1:]):
                        add(lvl, lvl * 1.001, "liqpool", f"EQH {tag} (sweep)", 1.8, "short")
                    if not up and lvl < price and all(r["l"] > lvl * 0.999 for r in rows[j + 1:]):
                        add(lvl * 0.999, lvl, "liqpool", f"EQL {tag} (sweep)", 1.8, "long")

    # ------------------------------------------------------------ calcul principal
    def compute(s):
        now = time.time()
        ref = s.last.get("binance") or s.last.get("coinbase")
        if not ref:
            return
        price, cand = ref[1], []

        def add(lo, hi, cat, label, w, d=None):
            d = d or ("long" if (lo + hi) / 2 < price else "short")
            if (d == "long" and lo < price) or (d == "short" and hi > price):
                cand.append(dict(lo=lo, hi=hi, cat=cat, label=label, w=w, dir=d))

        # 1) murs de liquidité persistants
        for side, k, q, age in s._walls(price, now):
            w = 1.5 + min(1.5, age / 300 * 1.5)
            add(k, k + WB, "wall", f"Mur {'bid' if side == 'bid' else 'ask'} {q:.0f} BTC ({age/60:.0f}min)", w,
                "long" if side == "bid" else "short")

        # 2) profil de volume 4h : POC / HVN
        m0 = int(now // 60) - 240
        vol = defaultdict(float)
        for m, d in s.prof.items():
            if m >= m0:
                for b, (bv, sv) in d.items():
                    vol[b] += bv + sv
        if vol:
            avg = sum(vol.values()) / len(vol)
            poc = max(vol, key=vol.get)
            for b, v in vol.items():
                if b == poc or v >= 1.8 * avg:
                    add(b, b + PB, "profile", "POC 4h" if b == poc else "HVN 4h", 1.3 if b == poc else 1.0)

        # 3) absorption (gros volume, peu de mouvement, delta opposé)
        bars = [(m, b) for m, b in sorted(s.bars.items()) if b[0] is not None and m >= int(now // 60) - 120]
        if len(bars) >= 30:
            vs = [b[4] for _, b in bars]
            mu = sum(vs) / len(vs)
            sd = (sum((x - mu) ** 2 for x in vs) / len(vs)) ** 0.5
            rg = sorted(b[1] - b[2] for _, b in bars)[len(bars) // 2]
            for _, b in bars[:-1]:
                if b[4] > mu + 1.5 * sd and (b[1] - b[2]) <= 1.2 * max(rg, 1):
                    r = b[5] / b[4]
                    if r < -0.2:
                        add(b[2], b[1], "absorb", "Absorption des ventes", 2.0, "long")
                    elif r > 0.2:
                        add(b[2], b[1], "absorb", "Absorption des achats", 2.0, "short")

        # 4) gros ordres agressifs (tape)
        cl = defaultdict(lambda: [0.0, 0.0])
        for ts, p, usd, sg in s.big:
            if now - ts < 7200:
                c = cl[int(p // PB) * PB]
                c[0] += sg * usd
                c[1] += usd
        for b, (net, tot) in cl.items():
            if tot >= 2 * LARGE_USD and abs(net) / tot > 0.5:
                below = b + PB / 2 < price
                if net > 0:
                    add(b, b + PB, "tape", "Gros achats défendus" if below else "Acheteurs piégés",
                        1.5 if below else 0.8, "long" if below else "short")
                else:
                    add(b, b + PB, "tape", "Vendeurs piégés" if below else "Gros ventes",
                        0.8 if below else 1.5, "long" if below else "short")

        # 5) clusters de liquidations (Binance perp + Bybit)
        lq = defaultdict(float)
        for ts, p, usd, long_liq in s.liq:
            if now - ts < 21600:
                lq[(int(p // PB) * PB, long_liq)] += usd
        for (b, long_liq), usd in lq.items():
            if usd >= LIQ_MIN and ((long_liq and b < price) or (not long_liq and b > price)):
                add(b, b + PB, "liq", f"Liquidations {'longs' if long_liq else 'shorts'} ${usd/1e6:.1f}M",
                    1.2 + min(1.8, usd / 3e6 * 1.8), "long" if long_liq else "short")

        # 6) structure de prix 15m / 1h
        if "15m" in s.candles:
            s._structure(s.candles["15m"], "15m", 1.5, price, add)
        if "1h" in s.candles:
            s._structure(s.candles["1h"], "1h", 2.0, price, add)

        zones = s._cluster(cand, price)

        # contexte : CVD par exchange, prime Coinbase/Binance
        mn = int(now // 60)
        cvd = {ex: round(sum(v for m, v in d.items() if m > mn - 15), 1) for ex, d in s.exd.items()}
        prem = None
        if "coinbase" in s.last and "binance" in s.last:
            prem = s.last["coinbase"][1] - s.last["binance"][1]
        tot = sum(cvd.values())
        bias = "haussier" if tot > 0 and (prem or 0) >= 0 else "baissier" if tot < 0 and (prem or 0) <= 0 else "neutre"
        s.snapshot = dict(
            ready=True, ts=now, price=price,
            zones={d: [z for z in zones if z["dir"] == d][:6] for d in ("long", "short")},
            ctx=dict(s.ctx, cvd15=cvd, premium=prem, bias=bias, spoof_pulled=s.pulled),
            warm=dict(minutes=len(bars), candles={k: len(v) for k, v in s.candles.items()},
                      books=list(s.books)))

    def _cluster(s, cand, price):
        out, tol = [], price * 0.0012
        for d in ("long", "short"):
            zs = sorted((c for c in cand if c["dir"] == d), key=lambda c: c["lo"] + c["hi"])
            groups = []
            for z in zs:
                g = groups[-1] if groups else None
                if g and z["lo"] <= g["hi"] + tol and max(g["hi"], z["hi"]) - g["lo"] <= price * 0.004:
                    g["lo"], g["hi"] = min(g["lo"], z["lo"]), max(g["hi"], z["hi"])
                    g["items"].append(z)
                else:
                    groups.append(dict(lo=z["lo"], hi=z["hi"], items=[z]))
            for g in groups:
                cats = {}
                for it in g["items"]:
                    cats[it["cat"]] = max(cats.get(it["cat"], 0), it["w"])
                if len(cats) < 2:
                    continue
                raw = sum(cats.values()) + 0.8 * (len(cats) - 1)
                score = min(100, round(raw / MAX_RAW * 100))
                mid = (g["lo"] + g["hi"]) / 2
                dist = abs(mid - price) / price * 100
                if dist > 3:
                    continue
                sl = g["lo"] * 0.999 if d == "long" else g["hi"] * 1.001
                risk = abs(mid - sl)
                sgn = 1 if d == "long" else -1
                out.append(dict(
                    dir=d, lo=round(g["lo"], 1), hi=round(g["hi"], 1), mid=round(mid, 1), score=score,
                    grade="A" if score >= 60 else "B" if score >= 40 else "C",
                    cats=sorted(cats), labels=sorted({i["label"] for i in g["items"]}), dist=round(dist, 2),
                    entry=round(mid, 1), sl=round(sl, 1),
                    tp1=round(mid + sgn * 1.5 * risk, 1), tp2=round(mid + sgn * 3 * risk, 1)))
        return sorted(out, key=lambda z: (-z["score"], z["dist"]))
