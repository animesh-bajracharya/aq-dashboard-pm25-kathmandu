import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ================= STREAMLIT CONFIG =================

st.set_page_config(
    page_title="Kathmandu Air Quality Tracker",
    page_icon="😷",
    layout="wide"
)

# Custom CSS to make metrics look better
st.markdown("""
<style>
    [data-testid="stMetricValue"] {
        font-size: 24px;
    }
</style>
""", unsafe_allow_html=True)

st.title("😷 Kathmandu Air Quality Tracker V 2.0")
st.markdown("### Daily Cycles & Actionable Insights")

DATA_FILE = Path("data/pm25_last_14_days.parquet")

# ================= HELPER FUNCTIONS =================

def get_aqi_color(pm25):
    """Returns color and text status based on PM2.5 value"""
    if pm25 <= 15: return "green", "Good (WHO Compliant)"
    if pm25 <= 35: return "orange", "Moderate (Sensitive Groups Alert)"
    if pm25 <= 55: return "red", "Unhealthy"
    return "violet", "Very Unhealthy"

def get_recommendation(pm25):
    if pm25 <= 15:
        return "✅ **Safe to Exercise:** Air quality is excellent. Great time for outdoor runs or ventilation."
    elif pm25 <= 35:
        return "⚠️ **Ventilate with Caution:** Okay for most, but sensitive groups should reduce exertion."
    elif pm25 <= 75:
        return "😷 **Wear a Mask:** Avoid prolonged outdoor exertion. Keep windows closed."
    else:
        return "⛔ **Stay Indoors:** Hazardous levels. Use air purifiers if available."

# ================= LOAD DATA =================

@st.cache_data(ttl=300)
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
df["date_str"] = df["timestamp_npt"].dt.strftime('%Y-%m-%d')

# ================= SIDEBAR CONTROLS =================

st.sidebar.header("Configuration")
days_option = st.sidebar.selectbox(
    "Select Analysis Period:",
    options=["Last 7 Days", "Last 14 Days"],
    index=0
)

# Determine cutoff
max_date = df["timestamp_npt"].max()
cutoff_days = 7 if days_option == "Last 7 Days" else 14
cutoff_date = max_date - pd.Timedelta(days=cutoff_days)
df_filtered = df[df["timestamp_npt"] > cutoff_date].copy()

# ================= KEY METRICS (KPIs) =================

# Calculate statistics
current_time_npt = datetime.now(timezone.utc) + timedelta(hours=5, minutes=45)
current_hour = current_time_npt.hour

hourly_means = df_filtered.groupby("hour")["value"].median()
current_hour_typical = hourly_means.get(current_hour, 0)
peak_hour = hourly_means.idxmax()
cleanest_hour = hourly_means.idxmin()

# Status Color
status_color, status_text = get_aqi_color(current_hour_typical)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label=f"Typical PM2.5 at {current_hour}:00",
        value=f"{current_hour_typical:.1f} µg/m³",
        delta="Based on recent trend",
        delta_color="off"
    )

with col2:
    st.metric(
        label="Worst Time of Day",
        value=f"{peak_hour}:00",
        delta=f"{hourly_means.max():.1f} µg/m³ avg",
        delta_color="inverse"
    )

with col3:
    st.metric(
        label="Best Time (Cleanest)",
        value=f"{cleanest_hour}:00",
        delta=f"{hourly_means.min():.1f} µg/m³ avg",
        delta_color="normal"
    )

with col4:
    st.markdown(f"**Current Status:**")
    st.markdown(f":{status_color}[**{status_text}**]")

# Recommendation Banner
rec_msg = get_recommendation(current_hour_typical)
st.info(rec_msg)

# ================= MAIN DIURNAL PLOT =================

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

fig = go.Figure()

# IQR Area
fig.add_trace(go.Scatter(
    x=hourly_stats["hour"], y=hourly_stats["q75"],
    line=dict(width=0), showlegend=False, hoverinfo="skip"
))
fig.add_trace(go.Scatter(
    x=hourly_stats["hour"], y=hourly_stats["q25"],
    fill="tonexty", fillcolor="rgba(100, 100, 100, 0.2)",
    line=dict(width=0), name="Typical Range (IQR)"
))

# Median Line
fig.add_trace(go.Scatter(
    x=hourly_stats["hour"], y=hourly_stats["median"],
    mode="lines+markers",
    line=dict(width=3, color="#FF4B4B"), # Streamlit Red
    name="Median PM2.5"
))

# Reference Lines
fig.add_hline(y=15, line_dash="dot", line_color="green", annotation_text="WHO (15)")
fig.add_hline(y=35, line_dash="dash", line_color="orange", annotation_text="Sensitive (35)")

# Current Time Marker
fig.add_vline(
    x=current_hour, line_dash="solid", line_color="black", opacity=0.3,
    annotation_text="NOW", annotation_position="top right"
)

fig.update_layout(
    title="<b>Diurnal Profile:</b> When does pollution spike?",
    xaxis_title="Hour of Day (NPT)",
    yaxis_title="PM2.5 (µg/m³)",
    xaxis=dict(dtick=1, fixedrange=True),
    yaxis=dict(range=[0, max(200, hourly_stats['q75'].max() + 20)]),
    legend=dict(orientation="h", y=1.1),
    hovermode="x unified",
    margin=dict(l=20, r=20, t=60, b=20)
)

st.plotly_chart(fig, use_container_width=True)

# ================= HEATMAP (New Feature) =================

st.markdown("### 📅 Pollution Intensity Heatmap")
st.caption("Identify streaks of bad air days versus isolated spikes.")

# Pivot data for heatmap: Index=Date, Columns=Hour, Values=Mean PM2.5
heatmap_data = df_filtered.groupby(["date_str", "hour"])["value"].mean().reset_index()
heatmap_pivot = heatmap_data.pivot(index="date_str", columns="hour", values="value")

fig_heat = px.imshow(
    heatmap_pivot,
    labels=dict(x="Hour of Day", y="Date", color="PM2.5"),
    x=heatmap_pivot.columns,
    y=heatmap_pivot.index,
    aspect="auto",
    color_continuous_scale="RdYlGn_r", # Red-Yellow-Green reversed
    origin='lower'
)

fig_heat.update_layout(
    xaxis=dict(dtick=1),
    margin=dict(l=20, r=20, t=20, b=20)
)

st.plotly_chart(fig_heat, use_container_width=True)

# ================= FOOTER =================
st.markdown("---")
st.caption(
    f"Last updated (NPT): {df['timestamp_npt'].max().strftime('%Y-%m-%d %H:%M')} | "
    "Data represents aggregation of all available sensors."
)
