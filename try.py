import requests
import threading
import time
import pymysql
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables
load_dotenv()

# CONFIGURATION
TAAPI_KEYS = [os.getenv("API_KEY1"), os.getenv("API_KEY2")]
df = pd.read_excel("binance_price_predictions_4hr_30072025_0700_to_30072025_1108.xlsx")
assets = df["ASSET (SYMBOL)"].dropna().tolist()
print(len(assets))