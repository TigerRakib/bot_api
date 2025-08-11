import requests
import threading
import time
import pymysql
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import pandas as pd
from collections import deque
import functools

# Load environment variables
load_dotenv()

# CONFIGURATION
TAAPI_KEYS = [os.getenv("API_KEY1"), os.getenv("API_KEY2")]
df = pd.read_excel("binance_price_predictions_4hr_30072025_0700_to_30072025_1108.xlsx") # Given Excel file
assets = df["ASSET (SYMBOL)"].dropna().tolist()

# Extract symbol and append "/USDT"
formatted_assets = [asset.split("(")[-1].replace(")", "").strip() + "/USDT" for asset in assets]

# Database config
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME")
}

# Retry decorator for robust retries
def retry(max_attempts=3, initial_delay=2, backoff=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"[RETRY] {func.__name__} attempt {attempt+1}/{max_attempts} failed: {e}")
                    if attempt + 1 == max_attempts:
                        print(f"[RETRY] {func.__name__} exhausted all retries.")
                        raise
                    time.sleep(delay)
                    delay *= backoff
        return wrapper
    return decorator

# Rate limiting per API key (max 5 requests per second)
MAX_REQS_PER_SECOND = 5
key_request_times = {key: deque() for key in TAAPI_KEYS}

def rate_limit(api_key):
    now = time.time()
    times = key_request_times[api_key]
    while times and now - times[0] > 1:
        times.popleft()
    if len(times) >= MAX_REQS_PER_SECOND:
        sleep_time = 1 - (now - times[0])
        if sleep_time > 0:
            time.sleep(sleep_time)
    times.append(time.time())

@retry()
def get_current_price(asset_string):
    try:
        db_api = "https://backend.mytradegenius.com/price/four-hour-prediction"
        response = requests.get(db_api, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "/" in asset_string:
            symbol = asset_string.split("/")[0].upper()

        api_symbols = [item.get("symbol", "").upper() for item in data]

        if symbol not in api_symbols:
            print(f"[WARN] Symbol '{symbol}' NOT found in API response symbols.")
            return None

        for item in data:
            if item.get("symbol", "").upper() == symbol:
                return item.get("current_price")
        return None

    except Exception as e:
        print(f"[ERROR] Getting current price for '{asset_string}': {e}")
        return None

@retry()
def save_signal_to_db(symbol, signal_type, current_price, strength):
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            signal_time = datetime.now(timezone.utc)
            cursor.execute("""
                INSERT INTO asset_signals (symbol, signal_type, current_price, signal_update_time, strength)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    current_price = VALUES(current_price),
                    signal_update_time = VALUES(signal_update_time),
                    strength = VALUES(strength),
                    signal_type = VALUES(signal_type)
            """, (symbol, signal_type, current_price, signal_time, strength))
            connection.commit()
        connection.close()
    except Exception as e:
        print(f"[ERROR] Saving signal to the Database for {symbol}: {e}")

@retry()
def fetch_indicators_batch(symbol, api_key, indicator_type):
    import time

    interval = "1m"
    exchange = "binance"

    if indicator_type in ("buy_sell", "hold_exit"):
        indicators = [
            ("rsi", {"optInTimePeriod": 14}),
            ("macd", {"optInFastPeriod": 12, "optInSlowPeriod": 26, "optInSignalPeriod": 9}),
            ("ema", {"optInTimePeriod": 9}),
            ("ema", {"optInTimePeriod": 21}),
            ("adx", {"optInTimePeriod": 14}),
            ("stochrsi", {"optInFastKPeriod": 14, "optInFastDPeriod": 3}),
            ("bbands", {"optInTimePeriod": 20, "optInNbDevUp": 2, "optInNbDevDn": 2}),
            ("vwma", {"period": 20}),
        ]
    else:
        print(f"[ERROR] Not a valid indicator_type: {indicator_type}")
        return {}

    base_url = "https://api.taapi.io"
    data_map = {}
    ema_count = 0

    for indicator_name, params in indicators:
        rate_limit(api_key)  # <-- rate limit before each request

        url = f"{base_url}/{indicator_name}"
        query = {
            "secret": api_key,
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
        }
        query.update(params)

        try:
            response = requests.get(url, params=query, timeout=10)
            response.raise_for_status()
            result = response.json()

            if indicator_name == "ema":
                key = "ema" if ema_count == 0 else f"ema_{ema_count}"
                data_map[key] = result
                ema_count += 1
            else:
                data_map[indicator_name] = result

            time.sleep(0.2)

        except Exception as e:
            print(f"[ERROR] Fetching {indicator_name} for {symbol}: {e}")

    return data_map

def heuristic_signal_voting(buy_score, sell_score):
    if buy_score >= 4 and buy_score > sell_score:
        return "Strong Buy", buy_score * 25
    elif sell_score >= 4 and sell_score > buy_score:
        return "Strong Sell", sell_score * 25
    elif buy_score > sell_score:
        return "Buy", buy_score * 20
    elif sell_score > buy_score:
        return "Sell", sell_score * 20
    else:
        return "Hold", 50

def evaluate_signal_by_type(symbol, data, indicator_type):
    try:
        rsi = data.get("rsi", {}).get("value", 50)
        macd_data = data.get("macd", {})
        macd_val = macd_data.get("valueMACD", 0)
        macd_signal = macd_data.get("valueMACDSignal", 0)
        ema9 = data.get("ema", {}).get("value", 0)
        ema21 = data.get("ema_1", {}).get("value", 0)
        adx_data = data.get("adx", {})
        adx = adx_data.get("value", 0)
        plus_di = adx_data.get("plusDI", 0)
        minus_di = adx_data.get("minusDI", 0)
        stochrsi_data = data.get("stochrsi", {})
        k = stochrsi_data.get("valueFastK", 50)
        d = stochrsi_data.get("valueFastD", 50)
        bbands_data = data.get("bbands", {})
        lower_band = bbands_data.get("valueLowerBand", 0)
        upper_band = bbands_data.get("valueUpperBand", 0)
        price = bbands_data.get("valueMiddleBand", 0)
        volume = data.get("vwma", {}).get("value", 0)

        if indicator_type == "buy_sell":
            buy_score = 0
            sell_score = 0

            if rsi < 35: buy_score += 1
            if macd_val > macd_signal and macd_val < 0: buy_score += 1
            if ema9 > ema21: buy_score += 1
            if adx > 25 and plus_di > minus_di: buy_score += 1
            if k > d and k < 20: buy_score += 1
            if price < lower_band and volume > 0: buy_score += 1

            if rsi > 65: sell_score += 1
            if macd_val < macd_signal and macd_val > 0: sell_score += 1
            if ema9 < ema21: sell_score += 1
            if adx > 25 and minus_di > plus_di: sell_score += 1
            if k < d and k > 80: sell_score += 1
            if price > upper_band and volume > 0: sell_score += 1

            return heuristic_signal_voting(buy_score, sell_score)

        elif indicator_type == "hold_exit":
            macd_flat = abs(macd_val - macd_signal) < 0.1
            ema_close = (ema21 != 0) and (abs(ema9 - ema21) / ema21 < 0.01)
            price_within_bbands = lower_band < price < upper_band
            volume_normal = volume > 0

            hold_condition = (
                40 <= rsi <= 60 and macd_flat and ema_close and
                adx < 20 and price_within_bbands and volume_normal
            )

            if hold_condition:
                return "Hold", 50
            else:
                return "Exit", 40

        else:
            print(f"[ERROR] Invalid indicator_type: {indicator_type}")
            return None, None

    except Exception as e:
        print(f"[ERROR] Evaluating signal for {symbol}: {e}")
        return None, None

def process_assets_all(frequency, indicator_type):
    key_index = 0
    while True:
        processed = 0
        for i, symbol in enumerate(formatted_assets):
            api_key = TAAPI_KEYS[key_index % len(TAAPI_KEYS)]
            key_index += 1

            current_price = get_current_price(symbol)
            if current_price is None:
                continue

            data = fetch_indicators_batch(symbol, api_key, indicator_type)
            if not data:
                continue

            signal, strength = evaluate_signal_by_type(symbol, data, indicator_type)
            if signal:
                asset_name=assets[i]
                print(f"[{signal}] {asset_name} @ {current_price} | Strength: {strength} | Time: {datetime.now(timezone.utc)}")
                save_signal_to_db(asset_name, signal, current_price, strength)

            processed += 1

        print(f"[{indicator_type.upper()}] Completed processing {processed} assets at {datetime.now(timezone.utc)}")
        time.sleep(frequency)

def start_threads():
    # Buy/Sell thread (every 60 seconds)
    threading.Thread(target=process_assets_all, args=(60, "buy_sell"), daemon=True).start()

    # Hold/Exit thread (every 600 seconds, 30s delay start)
    def hold_exit_runner():
        time.sleep(30)
        process_assets_all(600, "hold_exit")
    threading.Thread(target=hold_exit_runner, daemon=True).start()

if __name__ == "__main__":
    print("Starting signal processing threads...")
    start_threads()

    # Keep main thread alive
    while True:
        time.sleep(1)
