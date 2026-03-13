import duckdb
import os
import streamlit as pd
from streamlit_autorefresh import st_autorefresh

# Path to DuckDB relative to dashboard execution
DB_PATH = "data/school_data.duckdb"

def get_connection():
    """ Returns a read-only DuckDB connection safely. """
    # Since DuckDB 0.10.0 allows concurrent reads with read_only=True
    if os.path.exists(DB_PATH):
        try:
            return duckdb.connect(DB_PATH, read_only=True)
        except Exception as e:
            return None
    return None

def setup_autorefresh(interval_ms=5000):
    """
    Sets up auto-refresh and returns how many times it has refreshed.
    """
    return st_autorefresh(interval=interval_ms, key="data_refresh")

def get_data(query, conn):
    """ Executes a query and returns a pandas DataFrame. """
    if conn is None:
        return None
    try:
        return conn.execute(query).df()
    except Exception as e:
        return None
