import os
import time
import uuid
import base64
import traceback
from datetime import datetime, timezone
from typing import Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TRIGGER_SECRET        = os.getenv("TRIGGER_SECRET", "change-me")
KALSHI_KEY_ID         = os.getenv("KALSHI_API_KEY_ID", "")
KALSHI_KEY_PATH       = os.getenv("KALSHI_PRIVATE_KEY_PATH", "kalshi_private_key.pem")
KALSHI_KEY_CONTENTS   = os.getenv("KALSHI_PRIVATE_KEY_CONTENTS", "")
KALSHI_BASE_URL       = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
DEFAULT_MARKET_TICKER = os.getenv("KALSHI_MARKET_TICKER", "")
DEFAULT_SIDE          = os.getenv("KALSHI_SIDE", "yes")
DEFAULT_COUNT         = int(os.getenv("KALSHI_COUNT", "1"))

trigger_log = []


def _load_private_key():
    if KALSHI_KEY_CONTENTS:
        key_str = KALSHI_KEY_CONTENTS.replace("\\n", "\n")
        return serialization.load_pem_private_key(
            key_str.encode(), password=None
        )
    with open(KALSHI_KEY_PATH, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def _kalshi_headers(method: str, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    msg = ts + method.upper() + path
    key = _load_private_key()
    sig = key.sign(
        msg.encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    sig_b64 = base64.b64encode(sig).decode()
    return {
        "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "Content-Type": "application/json",
    }


async def place_kalshi_order(ticker: str, side: str, count: int) -> dict:
    path = "/portfolio/orders"
    body = {
        "ticker": ticker,
        "action": "buy",
        "side": side,
        "type": "market",
        "count": count,
        "client_order_id": str(uuid.uuid4()),
    }
    headers = _kalshi_headers("POST", path)
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            KALSHI_BASE_URL + path,
            headers=headers,
            json=body,
            timeout=10,
        )
        print(f"KALSHI RESPONSE: {resp.status_code} {resp.text}")
        resp.raise_for_status()
        return resp.json()


@app.post("/trigger")
async def trigger(
    x_api_key: Optional[str] = Header(None),
    ticker: Optional[str] = None,
    side: Optional[str] = None,
    count: Optional[int] = None,
):
    if x_api_key != TRIGGER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    market    = ticker or DEFAULT_MARKET_TICKER
    bet_side  = side or DEFAULT_SIDE
    bet_count = count or DEFAULT_COUNT

    if not market:
        raise HTTPException(status_code=400, detail="No market ticker configured")

    fired_at = datetime.now(timezone.utc).isoformat()

    try:
        result = await place_kalshi_order(market, bet_side, bet_count)
        entry = {"fired_at": fired_at, "market": market, "side": bet_side, "count": bet_count, "status": "ok", "order": result}
    except Exception as e:
        print(f"KALSHI ERROR: {str(e)}")
        print(traceback.format_exc())
        entry = {"fired_at": fired_at, "market": market, "side": bet_side, "count": bet_count, "status": "error", "error": str(e)}

    trigger_log.append(entry)
    return entry


@app.get("/log")
async def get_log(x_api_key: Optional[str] = Header(None)):
    if x_api_key != TRIGGER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return trigger_log[-50:]


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}