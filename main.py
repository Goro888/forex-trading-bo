import os
import asyncio
from fastapi import FastAPI, BackgroundTasks, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from metaapi_cloud_sdk import MetaApi

app = FastAPI(title="Live Multi-User Forex Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

active_bots = {}

class BotConfig(BaseModel):
    user_id: str
    mt_login: str
    mt_password: str
    mt_server: str
    platform: str = "mt5"
    symbol: str = "EURUSD"
    basket_profit: float = 50.0
    basket_loss: float = -100.0

async def run_user_trading_engine(config: BotConfig):
    token = os.getenv("METAAPI_TOKEN")
    if not token:
        active_bots[config.user_id]["status"] = "ERROR: Missing METAAPI_TOKEN secret"
        return

    api = MetaApi(token=token)

    try:
        account = await api.metatrader_account_api.create_account(account={
            'name': f"Live_{config.user_id}",
            'type': 'cloud',
            'login': config.mt_login,
            'password': config.mt_password,
            'server': config.mt_server,
            'platform': config.platform,
            'magic': 777111
        })

        await account.deploy()
        await account.wait_connected()

        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()

        active_bots[config.user_id]["status"] = "RUNNING"

        while active_bots.get(config.user_id, {}).get("running", False):
            positions = await connection.get_positions()
            current_pnl = sum(pos['profit'] for pos in positions)

            active_bots[config.user_id]["pnl"] = round(current_pnl, 2)
            active_bots[config.user_id]["open_trades"] = len(positions)

            if current_pnl >= config.basket_profit or current_pnl <= config.basket_loss:
                for pos in positions:
                    close_type = 'ORDER_TYPE_SELL' if pos['type'] == 'ORDER_TYPE_BUY' else 'ORDER_TYPE_BUY'
                    await connection.create_market_order(
                        symbol=pos['symbol'],
                        type=close_type,
                        volume=pos['volume']
                    )

            await asyncio.sleep(2)

    except Exception as e:
        active_bots[config.user_id]["status"] = f"ERROR: {str(e)}"
    finally:
        active_bots[config.user_id]["running"] = False

@app.get("/")
async def root():
    return {"status": "Forex bot engine is running."}

@app.post("/api/start")
async def start_bot(config: BotConfig, background_tasks: BackgroundTasks):
    active_bots[config.user_id] = {
        "running": True,
        "status": "CONNECTING TO BROKER",
        "pnl": 0.0,
        "open_trades": 0
    }
    background_tasks.add_task(run_user_trading_engine, config)
    return {"message": "Provisioning connection... Bot starting."}

@app.post("/api/stop/{user_id}")
async def stop_bot(user_id: str):
    if user_id in active_bots:
        active_bots[user_id]["running"] = False
    return {"message": "Stop signal sent."}

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    await websocket.accept()
    try:
        while True:
            data = active_bots.get(user_id, {"status": "OFFLINE", "pnl": 0.0, "open_trades": 0})
            await websocket.send_json(data)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
