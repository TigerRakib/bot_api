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
df = pd.read_excel("binance_price_predictions_4hr_30072025_0700_to_30072025_1108.xlsx")  # Your Excel file
assets = df["ASSET (SYMBOL)"].dropna().tolist()

# Extract symbol and append "/USDT"
formatted_assets = [asset.split("(")[-1].replace(")", "").strip() + "/USDT" for asset in assets]

# Database config
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME"),
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True,
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
    # Remove timestamps older than 1 second
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
        else:
            symbol = asset_string.upper()

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
def save_signal_to_db(
    symbol: str,
    signal_type: str,
    strength: float,
    rsi: float,
    ema9: float,
    ema21: float,
    volatility_pct: float,
    last_signal: str,
    name: str,
    current_signal: str,
    user_ip: str,
    price: float,
    updated_at: datetime,
    timestamp: datetime,
    last_buy: datetime,
    last_buy_price: float,
    last_sell: datetime,
    last_sell_price: float,
    last_hold: datetime,
    last_hold_price: float,
    last_exit: datetime,
    last_exit_price: float,
    macd_value: float,
    macd_signal: float,
    macd_hist: float,
    macd_1h_value: float,
    macd_1h_signal: float,
    macd_1h_hist: float,
    stochrsi_k: float,
    stochrsi_d: float,
    volume: float,
    prev_volume: float,
    volume_change_pct: float
) -> None:
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO trading_signals (
                        symbol, signal_type, strength, rsi, ema9, ema21,
                        volatility_pct, last_signal, name, current_signal,
                        user_ip, price, updated_at, timestamp,
                        last_buy, last_buy_price, last_sell, last_sell_price,
                        last_hold, last_hold_price, last_exit, last_exit_price,
                        macd_value, macd_signal, macd_hist,
                        macd_1h_value, macd_1h_signal, macd_1h_hist,
                        stochrsi_k, stochrsi_d, volume, prev_volume, volume_change_pct
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        signal_type = VALUES(signal_type),
                        strength = VALUES(strength),
                        rsi = VALUES(rsi),
                        ema9 = VALUES(ema9),
                        ema21 = VALUES(ema21),
                        volatility_pct = VALUES(volatility_pct),
                        last_signal = VALUES(last_signal),
                        name = VALUES(name),
                        current_signal = VALUES(current_signal),
                        user_ip = VALUES(user_ip),
                        price = VALUES(price),
                        updated_at = VALUES(updated_at),
                        last_buy = VALUES(last_buy),
                        last_buy_price = VALUES(last_buy_price),
                        last_sell = VALUES(last_sell),
                        last_sell_price = VALUES(last_sell_price),
                        last_hold = VALUES(last_hold),
                        last_hold_price = VALUES(last_hold_price),
                        last_exit = VALUES(last_exit),
                        last_exit_price = VALUES(last_exit_price),
                        macd_value = VALUES(macd_value),
                        macd_signal = VALUES(macd_signal),
                        macd_hist = VALUES(macd_hist),
                        macd_1h_value = VALUES(macd_1h_value),
                        macd_1h_signal = VALUES(macd_1h_signal),
                        macd_1h_hist = VALUES(macd_1h_hist),
                        stochrsi_k = VALUES(stochrsi_k),
                        stochrsi_d = VALUES(stochrsi_d),
                        volume = VALUES(volume),
                        prev_volume = VALUES(prev_volume),
                        volume_change_pct = VALUES(volume_change_pct)
                """, (
                    symbol, signal_type, strength, rsi, ema9, ema21,
                    volatility_pct, last_signal, name, current_signal,
                    user_ip, price, updated_at, timestamp,
                    last_buy, last_buy_price, last_sell, last_sell_price,
                    last_hold, last_hold_price, last_exit, last_exit_price,
                    macd_value, macd_signal, macd_hist,
                    macd_1h_value, macd_1h_signal, macd_1h_hist,
                    stochrsi_k, stochrsi_d, volume, prev_volume, volume_change_pct
                ))
            connection.commit()
    except Exception as e:
        print(f"[ERROR] Saving signal to the Database for {symbol}: {e}")

@retry()
def fetch_indicators_batch(symbol, api_key, indicator_type):
    interval = "1m"
    exchange = "binance"

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

    base_url = "https://api.taapi.io"
    data_map = {}
    ema_count = 0

    for indicator_name, params in indicators:
        rate_limit(api_key)

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
                key = "ema9" if ema_count == 0 else "ema21"
                data_map[key] = result.get("value", 0)
                ema_count += 1
            elif indicator_name == "rsi":
                data_map["rsi"] = result.get("value", 0)
            elif indicator_name == "macd":
                data_map["macd_value"] = result.get("valueMACD", 0)
                data_map["macd_signal"] = result.get("valueMACDSignal", 0)
                data_map["macd_hist"] = result.get("valueMACDHist", 0)
            elif indicator_name == "adx":
                data_map["adx"] = result.get("value", 0)
                data_map["plus_di"] = result.get("plusDI", 0)
                data_map["minus_di"] = result.get("minusDI", 0)
            elif indicator_name == "stochrsi":
                data_map["stochrsi_k"] = result.get("valueFastK", 0)
                data_map["stochrsi_d"] = result.get("valueFastD", 0)
            elif indicator_name == "bbands":
                data_map["bbands_lower"] = result.get("valueLowerBand", 0)
                data_map["bbands_upper"] = result.get("valueUpperBand", 0)
                data_map["bbands_middle"] = result.get("valueMiddleBand", 0)
            elif indicator_name == "vwma":
                data_map["volume"] = result.get("value", 0)


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
        # Note: data is a flat dict now
        rsi = data.get("rsi", 50)
        macd_val = data.get("macd_value", 0)
        macd_signal = data.get("macd_signal", 0)
        ema9 = data.get("ema9", 0)
        ema21 = data.get("ema21", 0)
        adx = data.get("adx", 0)
        plus_di = data.get("plus_di", 0)
        minus_di = data.get("minus_di", 0)
        k = data.get("stochrsi_k", 50)
        d = data.get("stochrsi_d", 50)
        lower_band = data.get("bbands_lower", 0)
        upper_band = data.get("bbands_upper", 0)
        price = data.get("bbands_middle", 0)
        volume = data.get("volume", 0)

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

            indicators = fetch_indicators_batch(symbol, api_key, indicator_type)
            if not indicators:
                continue

            signal, strength = evaluate_signal_by_type(symbol, indicators, indicator_type)
            if signal:
                asset_name = assets[i]
                now = datetime.now(timezone.utc)
                print(f"[{signal}] {asset_name} @ {current_price} | Strength: {strength} | Time: {datetime.now(timezone.utc)}")
                save_signal_to_db(
                    symbol=asset_name,
                    signal_type=signal,
                    strength=strength,
                    rsi=indicators.get("rsi", 0),
                    ema9=indicators.get("ema9", 0),
                    ema21=indicators.get("ema21", 0),
                    volatility_pct=0,
                    last_signal=signal,
                    name=asset_name,
                    current_signal=signal,
                    user_ip="0.0.0.0",  # or your logic to get IP
                    price=current_price,
                    updated_at=now,
                    timestamp=now,
                    last_buy=None,
                    last_buy_price=0,
                    last_sell=None,
                    last_sell_price=0,
                    last_hold=None,
                    last_hold_price=0,
                    last_exit=None,
                    last_exit_price=0,
                    macd_value=indicators.get("macd_value", 0),
                    macd_signal=indicators.get("macd_signal", 0),
                    macd_hist=indicators.get("macd_hist", 0),
                    macd_1h_value=0,  # No 1h MACD in your data - set 0 or implement separately
                    macd_1h_signal=0,
                    macd_1h_hist=0,
                    stochrsi_k=indicators.get("stochrsi_k", 0),
                    stochrsi_d=indicators.get("stochrsi_d", 0),
                    volume=indicators.get("volume", 0),
                    prev_volume=0,  # add logic if you have previous volume stored somewhere
                    volume_change_pct=0,  # calculate if you want
                )
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
