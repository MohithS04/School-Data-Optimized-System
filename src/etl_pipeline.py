import time
import os
import duckdb

DB_PATH = "data/school_data.duckdb"
RAW_DIR = "data/raw"
ARCHIVE_DIR = "data/archive"

def setup_db(conn):
    # Base tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS attendance (
            event_id VARCHAR,
            student_id INTEGER,
            school_id INTEGER,
            timestamp TIMESTAMP,
            status VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS academics (
            event_id VARCHAR,
            student_id INTEGER,
            school_id INTEGER,
            timestamp TIMESTAMP,
            subject VARCHAR,
            score INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS behavior (
            event_id VARCHAR,
            student_id INTEGER,
            school_id INTEGER,
            timestamp TIMESTAMP,
            incident_type VARCHAR,
            severity VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_health (
            event_id VARCHAR,
            timestamp TIMESTAMP,
            component VARCHAR,
            status VARCHAR,
            latency_ms FLOAT
        )
    """)
    
def process_files():
    files = [f for f in os.listdir(RAW_DIR) if f.endswith('.jsonl')]
    if not files:
        return 0
        
    conn = duckdb.connect(DB_PATH)
    setup_db(conn)
    
    start_time = time.time()
    
    try:
        conn.execute("BEGIN TRANSACTION")
        
        # Load all JSONL files at once into a temporary table 
        # `read_json_auto` unifies the diverse JSON schemas by filling with NULLs where keys are missing
        # We ensure it reads as string where typing might be ambiguous, although auto-detect is very robust.
        conn.execute(f"CREATE TEMP TABLE raw_events AS SELECT * FROM read_json_auto('{RAW_DIR}/*.jsonl', format='newline_delimited')")
        
        # Insert into base tables
        conn.execute("""
            INSERT INTO attendance
            SELECT event_id, student_id, school_id, CAST(timestamp AS TIMESTAMP), status
            FROM raw_events WHERE event_type = 'attendance'
        """)
        
        conn.execute("""
            INSERT INTO academics
            SELECT event_id, student_id, school_id, CAST(timestamp AS TIMESTAMP), subject, CAST(score AS INTEGER)
            FROM raw_events WHERE event_type = 'academic'
        """)
        
        conn.execute("""
            INSERT INTO behavior
            SELECT event_id, student_id, school_id, CAST(timestamp AS TIMESTAMP), incident_type, severity
            FROM raw_events WHERE event_type = 'behavior'
        """)
        
        conn.execute("""
            INSERT INTO system_health
            SELECT event_id, CAST(timestamp AS TIMESTAMP), component, status, CAST(latency_ms AS FLOAT)
            FROM raw_events WHERE event_type = 'system_health'
        """)
        
        # Rebuild aggregation tables
        conn.execute("""
            CREATE OR REPLACE TABLE daily_attendance_summary AS 
            SELECT 
                school_id,
                CAST(timestamp AS DATE) as date,
                COUNT(CASE WHEN status = 'Present' THEN 1 END) as present_count,
                COUNT(CASE WHEN status = 'Absent' THEN 1 END) as absent_count,
                COUNT(CASE WHEN status = 'Late' THEN 1 END) as late_count,
                COUNT(*) as total_students
            FROM attendance
            GROUP BY school_id, CAST(timestamp AS DATE)
        """)
        
        conn.execute("""
            CREATE OR REPLACE TABLE school_performance_summary AS 
            SELECT 
                school_id,
                subject,
                CAST(timestamp AS DATE) as date,
                AVG(score) as avg_score,
                MAX(score) as max_score,
                MIN(score) as min_score,
                COUNT(*) as assessments_taken
            FROM academics
            GROUP BY school_id, subject, CAST(timestamp AS DATE)
        """)
        
        conn.execute("""
            CREATE OR REPLACE TABLE recent_behavior_summary AS 
            SELECT 
                school_id,
                incident_type,
                severity,
                COUNT(*) as incident_count
            FROM behavior
            GROUP BY school_id, incident_type, severity
        """)
        
        # Record ETL Health
        latency = (time.time() - start_time) * 1000
        conn.execute(f"""
            INSERT INTO system_health VALUES (
                uuid(), current_timestamp, 'ETLPipeline', 'OK', {latency}
            )
        """)
        
        conn.execute("""
            CREATE OR REPLACE TABLE system_health_summary AS
            SELECT
                component,
                status,
                COUNT(*) as event_count,
                AVG(latency_ms) as avg_latency
            FROM system_health
            GROUP BY component, status
        """)
        
        conn.execute("COMMIT")
        
        # Move processed files
        for f in files:
            os.rename(os.path.join(RAW_DIR, f), os.path.join(ARCHIVE_DIR, f))
            
    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"ETL Error: {e}")
    finally:
        conn.close()
        
    return len(files)

def main():
    print("Starting ETL Pipeline...")
    try:
        while True:
            processed = process_files()
            if processed > 0:
                print(f"Processed {processed} tracking files and updated summary tables.")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping ETL pipeline.")

if __name__ == "__main__":
    main()
