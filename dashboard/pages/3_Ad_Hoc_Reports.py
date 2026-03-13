import streamlit as st
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_connection, get_data

st.set_page_config(page_title="Ad-Hoc Reporting", page_icon="📑", layout="wide")

st.title("📑 Ad-Hoc Reporting Tool")
st.markdown("Query the real-time schooling data store for customized operational reports.")

conn = get_connection()
if conn is None:
    st.error("Waiting for database connection...")
    st.stop()

# Define the reporting interface
report_type = st.selectbox("Select Report Category", ["Academics", "Attendance", "Behavior"])

col1, col2, col3 = st.columns(3)

with col1:
    school_filter = st.selectbox("School ID", ["All", "1", "2", "3", "4", "5"])
with col2:
    if report_type == "Academics":
        subject_filter = st.selectbox("Subject", ["All", "Math", "Science", "English", "History", "Art"])
    elif report_type == "Behavior":
        severity_filter = st.selectbox("Severity", ["All", "Low", "Medium", "High"])
    else:
        status_filter = st.selectbox("Status", ["All", "Present", "Absent", "Late"])
with col3:
    limit = st.number_input("Max Results", min_value=10, max_value=5000, value=100)

if st.button("Generate Report"):
    with st.spinner("Executing optimized query..."):
        query = ""
        where_clauses = []
        
        if school_filter != "All":
            where_clauses.append(f"school_id = {school_filter}")
            
        if report_type == "Academics":
            if subject_filter != "All":
                 where_clauses.append(f"subject = '{subject_filter}'")
            where_str = " AND ".join(where_clauses)
            where_str = f"WHERE {where_str}" if where_str else ""
            query = f"SELECT * FROM academics {where_str} ORDER BY timestamp DESC LIMIT {limit}"
            
        elif report_type == "Behavior":
            if severity_filter != "All":
                 where_clauses.append(f"severity = '{severity_filter}'")
            where_str = " AND ".join(where_clauses)
            where_str = f"WHERE {where_str}" if where_str else ""
            query = f"SELECT * FROM behavior {where_str} ORDER BY timestamp DESC LIMIT {limit}"
            
        elif report_type == "Attendance":
            if status_filter != "All":
                 where_clauses.append(f"status = '{status_filter}'")
            where_str = " AND ".join(where_clauses)
            where_str = f"WHERE {where_str}" if where_str else ""
            query = f"SELECT * FROM attendance {where_str} ORDER BY timestamp DESC LIMIT {limit}"
            
        df_result = get_data(query, conn)
        
        if df_result is not None and not df_result.empty:
            st.success(f"Returned {len(df_result)} records.")
            st.dataframe(df_result, use_container_width=True)
            
            csv = df_result.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{report_type.lower()}_report.csv",
                mime='text/csv',
            )
        else:
            st.warning("No records found matching the criteria.")

conn.close()
