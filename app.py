import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ================= STREAMLIT CONFIG =================

st.set_page_config(
    page_title="PM2.5 Diurnal Pattern – Kathmandu",
    layout="wide"
)

st.title("PM2.5 Diurnal Pattern - Kathmandu (V 1.5)")
st.caption("Hourly PM2.5 distribution across all stations (Nepal Time)")

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

# Ensure UTC
df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

# Convert to Nepal Time (UTC +5:45)
df["timestamp_npt"] = df["timestamp_utc"] + pd.Timedelta(hours=5, minutes=45)
df["hour"] = df["timestamp_npt"].dt.hour

# ================= DATA FILTERING =================

# Layout for controls
col1, col2 = st.columns([1, 3])

with col1:
    days_option = st.selectbox(
        "Select Time Range:",
        options=["Last 7 Days", "Last 14 Days"],
        index=0  # Defaults to "Last 7 Days"
    )

# Determine cutoff date based on selection
# We calculate relative to the latest data point available to ensure the chart isn't empty
max_date = df["timestamp_npt"].max()

if days_option == "Last 7 Days":
    cutoff_date = max_date - pd.Timedelta(days=7)
else:
    cutoff_date = max_date - pd.Timedelta(days=14)

# Filter the dataframe
df_filtered = df[df["timestamp_npt"] > cutoff_date].copy()

# ================= AGGREGATION =================

hourly_stats = (
    df_filtered
    .groupby("hour")["value"]
    .agg(
        median="median",
        q25=lambda x: x.quantile(0.25),
        q75=lambda x: x.quantile(0.75)
    )
    .reset_index()
)

# ================= PLOTLY OBJECT PLOT =================

fig = go.Figure()

# IQR band (Upper Bound) - Invisible line for fill
fig.add_trace(go.Scatter(
    x=hourly_stats["hour"],
    y=hourly_stats["q75"],
    line=dict(width=0),
    showlegend=False,
    hoverinfo="skip"
))

# IQR band (Lower Bound) - Fill to previous
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
    line=dict(width=3, color="red"),
    name="Median PM2.5"
))

# WHO 24-hour guideline (15 µg/m³)
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
    line_color="orange",
    annotation_text="Unhealthy for Sensitive (35)",
    annotation_position="top left"
)

# ================= CURRENT TIME INDICATOR =================
# Calculate current Nepal Time
current_time_npt = datetime.now(timezone.utc) + timedelta(hours=5, minutes=45)
current_hour = current_time_npt.hour

fig.add_vline(
    x=current_hour,
    line_dash="dash",
    line_color="black",
    annotation_text=f"Now ({current_hour}:00)",
    annotation_position="top"
)

# ================= LAYOUT =================
fig.update_layout(
    title=f"Hourly PM2.5 Pattern ({days_option})",
    xaxis_title="Hour of Day (Nepal Time)",
    yaxis_title="PM2.5 (µg/m³)",
    xaxis=dict(dtick=1),
    yaxis=dict(range=[0, 200]),
    template="plotly_white",
    legend=dict(orientation="h", y=-0.2),
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)

# ================= SUMMARY TABLE =================

with st.expander("View Detailed Statistics Table"):
    st.subheader(f"Hourly Summary Statistics ({days_option})")

    summary = (
        df_filtered
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
    f"Data source range: {df_filtered['timestamp_npt'].min()} to {df_filtered['timestamp_npt'].max()} (NPT)"
)
