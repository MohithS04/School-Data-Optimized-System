import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# Ensure utils is accessible
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_connection, setup_autorefresh, get_data

st.set_page_config(page_title="Executive Overview", page_icon="📊", layout="wide")

# Auto-refresh every 5 seconds
setup_autorefresh(2000)

st.title("📊 Executive Overview")
st.markdown("Live KPIs and Network-wide Schooling Insights")

conn = get_connection()
if conn is None:
    st.error("Waiting for Data Pipeline. Please ensure ETL and Generator are running...")
    st.stop()
    
# Layout filters
school_filter = st.sidebar.selectbox("Filter by School ID", ["All"] + [str(i) for i in range(1, 6)])

where_clause = ""
if school_filter != "All":
    where_clause = f"WHERE school_id = {int(school_filter)}"

with st.container():
    col1, col2, col3 = st.columns(3)
    
    # KPI 1: Total Attendance Today (approx)
    att_df = get_data(f"SELECT sum(present_count) as p, sum(total_students) as t FROM daily_attendance_summary {where_clause}", conn)
    if att_df is not None and not att_df.empty and att_df['t'].iloc[0]:
        rate = att_df['p'].iloc[0] / att_df['t'].iloc[0] * 100
        col1.metric("Overall Attendance Rate (Today)", f"{rate:.1f}%")
    else:
        col1.metric("Overall Attendance Rate", "N/A")
        
    # KPI 2: Total Behavioral Incidents Today
    inc_df = get_data(f"SELECT sum(incident_count) as c FROM recent_behavior_summary {where_clause}", conn)
    if inc_df is not None and not inc_df.empty and not pd.isna(inc_df['c'].iloc[0]):
        col2.metric("Total Behavior Incidents Today", int(inc_df['c'].iloc[0]))
    else:
        col2.metric("Total Behavior Incidents Today", 0)
        
    # KPI 3: Average Academic Score
    acad_df = get_data(f"SELECT avg(avg_score) as a FROM school_performance_summary {where_clause}", conn)
    if acad_df is not None and not acad_df.empty and not pd.isna(acad_df['a'].iloc[0]):
        col3.metric("Network Avg Assessment Score", f"{acad_df['a'].iloc[0]:.1f}")
    else:
        col3.metric("Network Avg Assessment Score", "N/A")

st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Attendance by School")
    df_att = get_data("SELECT school_id, sum(present_count) as present, sum(absent_count) as absent FROM daily_attendance_summary GROUP BY school_id", conn)
    if df_att is not None and not df_att.empty:
        df_att['School'] = "School " + df_att['school_id'].astype(str)
        fig = px.bar(df_att, x='School', y=['present', 'absent'], title="Present vs Absent Counts", barmode='group')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No attendance data")

with col2:
    st.subheader("Behavior Breakdown by Severity")
    df_beh = get_data(f"SELECT severity, sum(incident_count) as count FROM recent_behavior_summary {where_clause} GROUP BY severity", conn)
    if df_beh is not None and not df_beh.empty:
         fig2 = px.pie(df_beh, names='severity', values='count', title="Disciplinary Incidents (Severity)", hole=0.4, 
                       color='severity', color_discrete_map={'Low': 'green', 'Medium': 'orange', 'High': 'red'})
         st.plotly_chart(fig2, use_container_width=True)
    else:
         st.info("No behavior data")

st.subheader("Subject Performance")
df_sub = get_data(f"SELECT subject, avg(avg_score) as mean_score FROM school_performance_summary {where_clause} GROUP BY subject", conn)
if df_sub is not None and not df_sub.empty:
    fig3 = px.line(df_sub.sort_values('subject'), x='subject', y='mean_score', title="Average Score by Subject", markers=True)
    st.plotly_chart(fig3, use_container_width=True)

conn.close()
