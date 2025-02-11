import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from typing import Dict, List, Tuple
import difflib
import warnings
warnings.filterwarnings('ignore')

def create_db_connection():
    """Create database connection using credentials from Streamlit secrets."""
    try:
        conn = psycopg2.connect(
            dbname=st.secrets["postgres"]["dbname"],
            user=st.secrets["postgres"]["user"],
            password=st.secrets["postgres"]["password"],
            host=st.secrets["postgres"]["host"],
            port=st.secrets["postgres"]["port"]
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None

def get_column_mappings_from_db(conn) -> Dict[str, str]:
    """Fetch existing column mappings from RevReport table."""
    try:
        query = "SELECT report_col, required_col FROM RevReport"
        with conn.cursor() as cur:
            cur.execute(query)
            mappings = {row[0]: row[1] for row in cur.fetchall()}
        return mappings
    except Exception as e:
        st.error(f"Error fetching column mappings: {str(e)}")
        return {}
