import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# Ensure utils is accessible
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_connection, setup_autorefresh, get_data

st.set_page_config(page_title="System Health & Integrity", page_icon="⚙️", layout="wide")

setup_autorefresh(2000)

st.title("⚙️ System Health & Integrity")
st.markdown("Monitor the health, latency, and reliability of the data optimization pipeline.")

conn = get_connection()
if conn is None:
    st.error("Waiting for database connection...")
    st.stop()

# Summarize overall health
df_health = get_data("SELECT component, status, sum(event_count) as total_events, avg(avg_latency) as current_latency FROM system_health_summary GROUP BY component, status", conn)

if df_health is not None and not df_health.empty:
    st.subheader("Component Health Matrix")
    
    # Pivot for display
    pivot_health = df_health.pivot(index='component', columns='status', values='total_events').fillna(0)
    st.dataframe(pivot_health.style.highlight_max(axis=0), use_container_width=True)
    
    st.subheader("Pipeline Latency (ms)")
    # Average Latency Bar Chart
    lat_chart = px.bar(df_health[df_health['status'] == 'OK'], x='component', y='current_latency', 
                       title="Average Processing Latency by Component", text_auto=True)
    lat_chart.update_traces(marker_color='royalblue')
    st.plotly_chart(lat_chart, use_container_width=True)
else:
    st.info("No system health data found.")

st.markdown("### Recent Error Logs")
# Pull the latest errors or warnings from the raw table
df_errors = get_data("SELECT timestamp, component, status, latency_ms FROM system_health WHERE status IN ('Warning', 'Error') ORDER BY timestamp DESC LIMIT 10", conn)

if df_errors is not None and not df_errors.empty:
    st.dataframe(df_errors, use_container_width=True)
else:
    st.success("No recent errors or warnings! System operating nominally.")

conn.close()
