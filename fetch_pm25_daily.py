import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
from collections import deque
import os

API_KEY = os.environ["OPENAQ_API_KEY"]

BASE_URL = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": API_KEY}

LATITUDE = 27.702286
LONGITUDE = 85.319805
RADIUS_METERS = 10_000
LIMIT = 1000
MAX_REQUESTS_PER_MINUTE = 50

DATA_FILE = "data/pm25_last_14_days.parquet"

# ---------------- RATE LIMIT ----------------
REQUEST_COUNT = 0
REQUEST_TIMESTAMPS = deque()

def throttled_get(url, params=None):
    global REQUEST_COUNT
    now = time.time()

    while REQUEST_TIMESTAMPS and now - REQUEST_TIMESTAMPS[0] > 60:
        REQUEST_TIMESTAMPS.popleft()

    if len(REQUEST_TIMESTAMPS) >= MAX_REQUESTS_PER_MINUTE:
        sleep_time = 60 - (now - REQUEST_TIMESTAMPS[0])
        if sleep_time > 0:
            time.sleep(sleep_time)

    REQUEST_COUNT += 1
    REQUEST_TIMESTAMPS.append(time.time())

    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r

# ---------------- DATE RANGE (UTC AWARE) ----------------
end_date = datetime.now(timezone.utc)
start_date = end_date - timedelta(days=1)

date_from = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
date_to = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------- FETCH LOCATIONS ----------------
locations = []
page = 1

while True:
    params = {
        "coordinates": f"{LATITUDE},{LONGITUDE}",
        "radius": RADIUS_METERS,
        "limit": LIMIT,
        "page": page
    }

    r = throttled_get(f"{BASE_URL}/locations", params)
    results = r.json().get("results", [])

    if not results:
        break

    locations.extend(results)

    if len(results) < LIMIT:
        break

    page += 1

# ---------------- FILTER PM2.5 SENSORS ----------------
sensors = []

for loc in locations:
    for s in loc.get("sensors", []):
        if s.get("parameter", {}).get("name") == "pm25":
            sensors.append({
                "sensor_id": s["id"],
                "location": loc["name"],
                "latitude": loc["coordinates"]["latitude"],
                "longitude": loc["coordinates"]["longitude"]
            })

# ---------------- FETCH MEASUREMENTS ----------------
records = []

for s in sensors:
    page = 1
    while True:
        params = {
            "datetime_from": date_from,
            "datetime_to": date_to,
            "limit": LIMIT,
            "page": page
        }

        r = throttled_get(
            f"{BASE_URL}/sensors/{s['sensor_id']}/measurements",
            params
        )

        results = r.json().get("results", [])
        if not results:
            break

        for row in results:
            records.append({
                "timestamp_utc": row["period"]["datetimeFrom"]["utc"],
                "value": row["value"],
                "sensor_id": s["sensor_id"],
                "location": s["location"],
                "latitude": s["latitude"],
                "longitude": s["longitude"]
            })

        if len(results) < LIMIT:
            break

        page += 1

# ---------------- DATAFRAME ----------------
new_df = pd.DataFrame(records)

os.makedirs("data", exist_ok=True)

if os.path.exists(DATA_FILE):
    old_df = pd.read_parquet(DATA_FILE)
    df = pd.concat([old_df, new_df], ignore_index=True)
else:
    df = new_df.copy()

# 🔑 FORCE datetime conversion AFTER concat
df["timestamp_utc"] = pd.to_datetime(
    df["timestamp_utc"],
    utc=True,
    errors="coerce"
)

# Drop bad timestamps if any
df = df.dropna(subset=["timestamp_utc"])

# ---------------- KEEP LAST 14 DAYS ----------------
cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=14)
df = df[df["timestamp_utc"] >= cutoff]


# ---------------- SAVE ----------------
df.to_parquet(DATA_FILE, index=False)
