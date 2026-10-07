import asyncio, logging, os, time
from contextlib import asynccontextmanager
from pathlib import Path
import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse

from .collectors import start_all
from .engine import Engine

logging.basicConfig(level=logging.INFO)
eng = Engine()
ALERT_MIN = int(os.getenv("ALERT_MIN", "60"))
TG_TOKEN, TG_CHAT = os.getenv("TG_TOKEN"), os.getenv("TG_CHAT")
sent = {}


async def tg(text):
    if TG_TOKEN and TG_CHAT:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", json={"chat_id": TG_CHAT, "text": text})


async def engine_loop():
    while True:
        try:
            eng.compute()
            snap = eng.snapshot
            if snap.get("ready"):
                for d, zs in snap["zones"].items():
                    for z in zs:
                        key = (d, round(z["mid"] / 100))
                        if z["score"] >= ALERT_MIN and z["dist"] <= 0.2 and time.time() - sent.get(key, 0) > 1800:
                            sent[key] = time.time()
                            await tg(f"BTC {d.upper()} zone {z['lo']}-{z['hi']} (score {z['score']}, {z['grade']})\n"
                                     f"Entrée {z['entry']} | SL {z['sl']} | TP1 {z['tp1']} | TP2 {z['tp2']}\n" + ", ".join(z["labels"]))
        except Exception:
            logging.exception("compute")
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app):
    tasks = start_all(eng) + [asyncio.create_task(engine_loop())]
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/healthz")
def health():
    return {"ok": True}


@app.get("/api/zones")
def zones():
    return eng.snapshot


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")
