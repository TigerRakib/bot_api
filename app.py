import os
import pymysql
from dotenv import load_dotenv
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import List, Dict, Any
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler



load_dotenv()
app = FastAPI()


# Database config from environment variables
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME")
}

# Mount static files (css/js)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def update_signals():
    print("Running signal update job at", datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M:%S"))
    # TODO: Add your update logic here, e.g., update DB or cache

scheduler = BackgroundScheduler()
scheduler.add_job(func=update_signals, trigger="interval", seconds=60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    print("Scheduler started")
    yield
    scheduler.shutdown()
    print("Scheduler stopped")

app.router.lifespan_context = lifespan

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("signal.html", {"request": request})

@app.get("/signals")
async def get_signals():
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM trading_signals ORDER BY symbol ASC")
            rows = cursor.fetchall()
        connection.close()

        json_compatible_data = jsonable_encoder(rows)
        return json_compatible_data
    except Exception as e:
        return {"error": str(e)}
    

@app.get("/signal/technical-indicators", response_class=JSONResponse)
async def get_technical_indicators() -> List[Dict[str, Any]]:
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM trading_signals ORDER BY symbol ASC")
            rows = cursor.fetchall()
        connection.close()

        # Optionally filter or transform data here before returning
        return rows

    except Exception as e:
        return {"error": str(e)}
