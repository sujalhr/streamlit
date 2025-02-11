import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from io import StringIO
import difflib
from typing import Dict, List, Tuple
import yaml
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

# Configuration and mappings
STANDARD_COLUMNS = {
    "eMonth": ["Month of Report", "month report", "report month", "month"],
    "country": ["Country", "country name", "region"],
    "targetingProduct": ["Product Name", "product", "product type"],
    "agencyOriginal": ["Adsquare Client", "client name", "agency"],
    "distribution": ["Activation Type", "activation", "distribution type"],
    "dspOriginal": ["Platform Partner Name (DSP)", "platform partner", "dsp name"],
    "monetisation": ["Monetisation Type", "monetization", "revenue type"],
    "segId": ["External Dataset ID", "dataset id", "external id"],
    "cpm": ["Net Dataset Price", "ATTRIBUTE CPM in EUR", "NET ATTRIBUTE CPM in EUR"],
    "impressions": ["Share of Quantity", "quantity share", "impressions"],
    "grossRev": ["Net Campaign Revenue", "campaign revenue", "gross revenue"],
    "netRev": ["Data Partner Revenue", "partner revenue", "net revenue"],
    "attributePath": ["Taxonomy", "attribute path", "taxonomy path"],
    "attributeName": ["Dataset Name", "dataset", "attribute name"]
}

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
        # Additional connection test
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()
            st.success(f"✅ Database Connection Successful! PostgreSQL Version: {version[0]}")
        
        return conn
    
    except Exception as e:
        st.error(f"❌ Database Connection Failed: {str(e)}")
        st.error("Please check your database credentials and network connection.")
        return None

def find_data_start(df: pd.DataFrame) -> int:
    """Find where the actual data begins in the Excel file."""
    for idx, row in df.iterrows():
        # Look for rows that might contain our expected columns
        if any(any(possible_name.lower() in str(cell).lower() 
               for possible_name in names_list) 
               for standard_name, names_list in STANDARD_COLUMNS.items() 
               for cell in row):
            return idx
    return 0

def get_best_column_match(column: str, standard_columns: Dict[str, List[str]]) -> Tuple[str, float]:
    """Find the best matching standard column name using fuzzy matching."""
    best_match = None
    best_ratio = 0
    
    column = column.lower().strip()
    
    for standard_name, variations in standard_columns.items():
        # Check exact matches first
        if column in [var.lower() for var in variations]:
            return standard_name, 1.0
            
        # Then try fuzzy matching
        for variation in variations:
            ratio = difflib.SequenceMatcher(None, column.lower(), 
                                          variation.lower()).ratio()
            if ratio > best_ratio and ratio > 0.8:  # 80% similarity threshold
                best_ratio = ratio
                best_match = standard_name
                
    return best_match, best_ratio

def process_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[str]], Dict]:
    """Process the dataframe and identify columns that need review."""
    # Define the standard column mapping
    COLUMN_MAPPING = {
        "Data Partner Name": "partner name",
        "Month of Report": "eMonth",
        "Country": "country",
        "Product Name": "targetingProduct",
        "Adsquare Client": "agencyOriginal",
        "Activation Type": "distribution",
        "Platform Partner Name (DSP)": "dspOriginal",
        "Monetisation Type": "monetisation",
        "Segment ID": "segId",
        "Segment Name": "segName",
        "External Dataset ID": "extDataId",
        "Report Currency": "curr",
        "Net Dataset Price": "price",
        "ATTRIBUTE CPM in EUR": "cpmAtt",
        "NET ATTRIBUTE CPM in EUR": "cpmNet",
        "Share of Quantity": "impressions",
        "Net Campaign Revenue": "grossRev",
        "Data Partner Revenue": "netRev",
        "Data Partner Revenue Share": "shareRev",
        "Taxonomy": "attributePath",
        "Dataset Name": "attributeName",
        "Price Type": "cmp"
    }
    
    # Find where data starts
    start_row = find_data_start(df)
    if start_row > 0:
        df = df.iloc[start_row:].reset_index(drop=True)
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)

    # Initialize dictionaries for matched and unmatched columns
    matched_columns = {}
    unmatched_columns = {}
    
    # Track which target columns have been used
    used_target_columns = set()
    
    # First pass: Match exact columns
    for col in df.columns:
        if col in COLUMN_MAPPING:
            matched_columns[col] = COLUMN_MAPPING[col]
            used_target_columns.add(COLUMN_MAPPING[col])
    
    # Second pass: Identify unmatched columns
    # Only offer remaining unused target columns as options
    remaining_targets = {v for v in COLUMN_MAPPING.values() if v not in used_target_columns}
    
    for col in df.columns:
        if col not in matched_columns:
            unmatched_columns[col] = list(remaining_targets)

    return df, unmatched_columns, matched_columns

def create_streamlit_app():
    st.title("Report Processing Application")
    
    uploaded_file = st.file_uploader("Upload your Excel report", type=['xlsx', 'xls'])
    
    if uploaded_file is not None:
        try:
            df = pd.read_excel(uploaded_file)
            
            # Process the dataframe
            df, unmatched_columns, matched_columns = process_dataframe(df)
            
            # Show sample of original data
            st.subheader("Sample of Uploaded Data")
            st.dataframe(df.head())
            
            # Add button to show matched columns
            if st.button("Show Matched Columns"):
                st.subheader("Automatically Matched Columns")
                for original, mapped in matched_columns.items():
                    st.text(f"{original} → {mapped}")
            
            # Initialize final mappings with matched columns
            final_mappings = matched_columns.copy()
            
            # Handle unmatched columns
            if unmatched_columns:
                st.subheader("Columns Requiring Mapping")
                st.warning("Please map the following unmatched columns:")
                
                # Create a dictionary to store the selected mappings
                selected_mappings = {}
                
                for col, possible_matches in unmatched_columns.items():
                    if possible_matches:  # Only show dropdown if there are possible matches
                        selected_mapping = st.selectbox(
                            f"Select mapping for '{col}'",
                            options=possible_matches,
                            key=f"select_{col}"
                        )
                        selected_mappings[col] = selected_mapping
                        final_mappings[col] = selected_mapping
                    else:
                        st.error(f"No available mappings for '{col}'")
            
                # Show preview button after all mappings are selected
                if st.button("Preview Processed Data"):
                    processed_df = df.rename(columns=final_mappings)
                    st.subheader("Preview of Processed Data")
                    st.dataframe(processed_df.head())
                    
                    # Save to database option
                    if st.button("Save to Database"):
                        try:
                            conn = create_db_connection()
                            if conn:
                                engine = create_engine(
                                    f'postgresql://{st.secrets["postgres"]["user"]}:'
                                    f'{st.secrets["postgres"]["password"]}@'
                                    f'{st.secrets["postgres"]["host"]}:'
                                    f'{st.secrets["postgres"]["port"]}/'
                                    f'{st.secrets["postgres"]["dbname"]}'
                                )
                                
                                processed_df.to_sql(
                                    'revenue_reports',
                                    engine,
                                    if_exists='append',
                                    index=False
                                )
                                
                                st.success("Data successfully saved to database!")
                                
                        except Exception as e:
                            st.error(f"Error saving to database: {str(e)}")
            else:
                st.success("All columns were successfully mapped!")
                if st.button("Preview Processed Data"):
                    processed_df = df.rename(columns=matched_columns)
                    st.subheader("Preview of Processed Data")
                    st.dataframe(processed_df.head())
                    
                    if st.button("Save to Database"):
                        try:
                            conn = create_db_connection()
                            if conn:
                                engine = create_engine(
                                    f'postgresql://{st.secrets["postgres"]["user"]}:'
                                    f'{st.secrets["postgres"]["password"]}@'
                                    f'{st.secrets["postgres"]["host"]}:'
                                    f'{st.secrets["postgres"]["port"]}/'
                                    f'{st.secrets["postgres"]["dbname"]}'
                                )
                                
                                processed_df.to_sql(
                                    'revenue_reports',
                                    engine,
                                    if_exists='append',
                                    index=False
                                )
                                
                                st.success("Data successfully saved to database!")
                                
                        except Exception as e:
                            st.error(f"Error saving to database: {str(e)}")
                        
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    create_streamlit_app()