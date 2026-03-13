# Real-Time Schooling Data Optimization System

## Overview
The **Real-Time Schooling Data Optimization System** is an end-to-end data pipeline and analytics platform designed for K-12 education data. It simulates real-time data ingestion for attendance, academic performance, and student behavior, processes the incoming data using an ELT (Extract, Load, Transform) approach, and provides an interactive Streamlit dashboard for real-time monitoring and reporting.

## Features & Components
1. **Real-Time Data Generator (`src/data_generator.py`)** 
   - Simulates streaming events (Attendance, Academic scores, Behavior incidents, and System Health).
   - Generates JSONL log files at regular intervals to mimic a continuous data feed.

2. **ETL / ELT Pipeline (`src/etl_pipeline.py`)**
   - Continuously monitors the raw data directory for new `.jsonl` files.
   - Loads the streaming data directly into a high-performance **DuckDB** database.
   - Computes aggregated tables on the fly (e.g., daily attendance, school performance summary, recent behavior incidents, system health summary).
   - Archives processed files to maintain a clean raw data landing zone.

3. **Streamlit Analytics Dashboard (`dashboard/` directory)**
   - **Executive Overview**: High-level KPIs and trends for school administrators.
   - **System Health**: Infrastructure monitoring, tracking ETL latency, and generator health.
   - **Ad-Hoc Reports**: Interactive exploration of various domains via customized queries.
   - Auto-refreshing UI to showcase real-time data updates dynamically.

## Tech Stack
- **Dashboard & UI**: [Streamlit](https://streamlit.io/), Plotly (for interactive charts), streamlit-autorefresh
- **Database / Data Processing**: [DuckDB](https://duckdb.org/), Pandas
- **Data Generation**: Python `Faker`
- **Language**: Python 3.9+

## Folder Structure
```
.
├── dashboard/
│   ├── app.py                      # Main entry point for Streamlit dashboard
│   ├── utils.py                    # Helper functions for the dashboard
│   └── pages/                      # Dashboard views
│       ├── 1_Executive_Overview.py
│       ├── 2_System_Health.py
│       └── 3_Ad_Hoc_Reports.py
├── data/
│   ├── raw/                        # Landing zone for real-time JSONL events
│   └── archive/                    # Archived JSONL files after ETL processing
├── src/
│   ├── data_generator.py           # Synthetic event producer script
│   └── etl_pipeline.py             # Event consumer and DuckDB loader
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation
```

## Setup & Installation

1. **Clone the Repository**
   ```bash
   git clone <your-repo-url>
   cd "Schooling Data Optimization System"
   ```

2. **Create a Virtual Environment (Optional but recommended)**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

To see the system in action, you need to run three separate processes concurrently. Open three separate terminal windows/tabs:

**Terminal 1: Start the Data Generator**
```bash
python src/data_generator.py
```
*This will start producing `.jsonl` files locally mimicking a live data stream.*

**Terminal 2: Start the ETL Pipeline**
```bash
python src/etl_pipeline.py
```
*This will ingest the new files into DuckDB and build necessary analytical views.*

**Terminal 3: Launch the Streamlit Dashboard**
```bash
streamlit run dashboard/app.py
```
*This will open the interactive dashboard in your browser. As the generator makes new data and the ETL processes it, the dashboard will auto-refresh to show real-time changes.*
