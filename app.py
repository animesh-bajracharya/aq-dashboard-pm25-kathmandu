import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

# ================= STREAMLIT CONFIG =================

st.set_page_config(
    page_title="PM2.5 Diurnal Pattern – Kathmandu",
    layout="wide"
)

st.title("PM2.5 Diurnal Pattern - Kathmandu (v1.0)")
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

# # ================= BOXPLOT DATA =================

# hourly_data = [
#     df[df["hour"] == h]["value"].dropna()
#     for h in range(24)
# ]

# ================ Plot with streamlit object ================
hourly_stats = (
    df
    .groupby("hour")["value"]
    .agg(
        median="median",
        q25=lambda x: x.quantile(0.25),
        q75=lambda x: x.quantile(0.75)
    )
    .reset_index()
)

fig = go.Figure()

# IQR band
fig.add_trace(go.Scatter(
    x=hourly_stats["hour"],
    y=hourly_stats["q75"],
    line=dict(width=0),
    showlegend=False,
    hoverinfo="skip"
))

fig.add_trace(go.Scatter(
    x=hourly_stats["hour"],
    y=hourly_stats["q25"],
    fill="tonexty",
    fillcolor="rgba(255,0,0,0.15)",
    line=dict(width=0),
    name="Typical Range (IQR)"
))

# Median line
fig.add_trace(go.Scatter(
    x=hourly_stats["hour"],
    y=hourly_stats["median"],
    mode="lines+markers",
    line=dict(width=3),
    name="Median PM2.5"
))

# WHO 24-hour guideline (15 µg/m³ – updated WHO)
fig.add_hline(
    y=15,
    line_dash="dot",
    line_color="green",
    annotation_text="WHO Guideline (15)",
    annotation_position="top left"
)

# Unhealthy threshold (commonly used ~35 µg/m³)
fig.add_hline(
    y=35,
    line_dash="dash",
    line_color="red",
    annotation_text="Avoid Outdoor Activity (35)",
    annotation_position="top left"
)
# ================= add current time
current_time_IST = datetime.now() + pd.Timedelta(hours=5, minutes=45)
current_hour = current_time_IST.hour

fig.add_vline(
    x=current_hour,
    line_dash="dash",
    line_color="black",
    annotation_text=f"Now ({current_hour}:00)",
    annotation_position="top"
)
# ==================== Final layout of the graph
fig.update_layout(
    title="Q: When Should You Avoid Outdoor Activity? (Hourly PM2.5 Pattern) Ans: Aim for a time close to WHO recommendated levels",
    xaxis_title="Hour of Day (Nepal Time)",
    yaxis_title="PM2.5 (µg/m³)",
    xaxis=dict(dtick=1),
    yaxis=dict(range=[0, 200]),
    template="plotly_white",
    legend=dict(orientation="h", y=-0.2)
)

st.plotly_chart(fig, use_container_width=True)

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
