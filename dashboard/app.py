import streamlit as st

st.set_page_config(
    page_title="Schooling Data Optimization System",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🎓 Schooling Data Optimization System")
st.markdown("### Real-Time Analytics Pipeline Dashboard")

st.markdown("""
Welcome to the Schooling Data Optimization System dashboard.
This system simulates real-time data ingestion and optimized ETL pipelines for better accessibility and speed, serving network leadership and operational staff.

**Features:**
- **Executive Overview**: High-level live KPIs and geographic filters for decision-making.
- **System Health**: Monitor data system health and integrity for reliable insights.
- **Ad-Hoc Reporting**: Streamlined data reporting processes for academic operations staff.

Use the sidebar to navigate between operational pages.
""")

st.info("The dashboard connects dynamically to the DuckDB backend, enabling millisecond refresh rates on continuous data streams.")
