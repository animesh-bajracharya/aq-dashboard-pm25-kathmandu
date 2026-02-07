import requests
import pandas as pd
from datetime import datetime, timedelta
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

REQUEST_COUNT = 0
REQUEST_TIMESTAMPS = deque()

def throttled_get(url, params=None):
    global REQUEST_COUNT
    now = time.time()

    while REQUEST_TIMESTAMPS and now - REQUEST_TIMESTAMPS[0] > 60:
        REQUEST_TIMESTAMPS.popleft()

    if len(REQUEST_TIMESTAMPS) >= MAX_REQUESTS_PER_MINUTE:
        time.sleep(60 - (now - REQUEST_TIMESTAMPS[0]))

    REQUEST_COUNT += 1
    REQUEST_TIMESTAMPS.append(time.time())

    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r

# --------- Date range: last 24 hours ----------
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=1)

date_from = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
date_to = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

# --------- Locations ----------
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

# --------- PM2.5 sensors ----------
sensors = []
for loc in locations:
    for s in loc.get("sensors", []):
        if s.get("parameter", {}).get("name") == "pm25":
            sensors.append({
                "sensor_id": s["id"],
                "location": loc["name"],
                "lat": loc["coordinates"]["latitude"],
                "lon": loc["coordinates"]["longitude"]
            })

# --------- Fetch data ----------
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
                "latitude": s["lat"],
                "longitude": s["lon"]
            })

        if len(results) < LIMIT:
            break
        page += 1

new_df = pd.DataFrame(records)
new_df["timestamp_utc"] = pd.to_datetime(new_df["timestamp_utc"], utc=True)

os.makedirs("data", exist_ok=True)

if os.path.exists(DATA_FILE):
    old_df = pd.read_parquet(DATA_FILE)
    df = pd.concat([old_df, new_df])
else:
    df = new_df

cutoff = datetime.utcnow() - timedelta(days=14)
df = df[df["timestamp_utc"] >= cutoff]

df.to_parquet(DATA_FILE, index=False)

print(f"Added {len(new_df)} rows")
print(f"Total stored: {len(df)}")
print(f"Requests used: {REQUEST_COUNT}")
