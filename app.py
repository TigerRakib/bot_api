from flask import Flask, jsonify, render_template
import pymysql
import os
from datetime import datetime,timezone
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME")
}

def update_signals():
    print("Running signal update job at", datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M:%S"))

scheduler = BackgroundScheduler()
scheduler.add_job(func=update_signals, trigger="interval", seconds=60)
scheduler.start()

@app.route("/")
def index():
    return render_template("signal.html")

@app.route("/api/signals")
def get_signals():
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM asset_signals ORDER BY signal_update_time DESC")
            rows = cursor.fetchall()
        connection.close()

        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
