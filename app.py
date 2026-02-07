import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ================= STREAMLIT CONFIG =================

st.set_page_config(
    page_title="PM2.5 Diurnal Pattern – Kathmandu",
    layout="wide"
)

st.title("PM2.5 Diurnal Pattern (Box & Whisker)")
st.caption(
    "Hourly PM2.5 distribution across all stations (rolling last 14 days, Nepal Time)"
)

DATA_FILE = Path("data/pm25_last_14_days.parquet")

# ================= LOAD DATA =================

@st.cache_data(ttl=300)  # refresh every 5 minutes
def load_data():
    if not DATA_FILE.exists():
        return pd.DataFrame()
    return pd.read_parquet(DATA_FILE)

df = load_data()

if df.empty:
    st.warning("No data available yet. Please check back later.")
    st.stop()

# ================= TIME HANDLING =================

df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

# Convert to Nepal Time (UTC +5:45)
df["timestamp_npt"] = df["timestamp_utc"] + pd.Timedelta(hours=5, minutes=45)

df["hour"] = df["timestamp_npt"].dt.hour

# ================= BOXPLOT DATA =================

hourly_data = [
    df[df["hour"] == h]["value"].dropna()
    for h in range(24)
]

# ================= PLOT =================

fig, ax = plt.subplots(figsize=(14, 6))

ax.boxplot(
    hourly_data,
    positions=range(24),
    widths=0.6,
    showfliers=False
)

ax.set_xlabel("Hour of Day (Nepal Time)")
ax.set_ylabel("PM2.5 (µg/m³)")
ax.set_title("Hourly PM2.5 Distribution (All Stations, All Days)")

ax.set_xticks(range(24))
ax.set_xticklabels(range(24))
ax.grid(axis="y", linestyle="--", alpha=0.4)

# ---- OPTIONAL: overlay hourly mean ----
hourly_mean = df.groupby("hour")["value"].mean()
ax.plot(range(24), hourly_mean, color="red", marker="o", label="Hourly Mean")
ax.legend()

st.pyplot(fig)

# ================= SUMMARY TABLE =================

st.subheader("Hourly Summary Statistics")

summary = (
    df
    .groupby("hour")["value"]
    .agg(
        count="count",
        mean="mean",
        median="median",
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75),
        min="min",
        max="max"
    )
    .round(2)
)

st.dataframe(summary, use_container_width=True)

# ================= FOOTER =================

st.caption(
    f"Last data timestamp (UTC): {df['timestamp_utc'].max()}"
)
