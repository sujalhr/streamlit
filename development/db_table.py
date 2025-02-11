import streamlit as st
import pandas as pd
import psycopg2
from io import StringIO
import warnings
warnings.filterwarnings('ignore')

def create_db_connection():
    """Establish a database connection."""
    try:
        conn = psycopg2.connect(
            dbname=st.secrets["postgres"]["dbname"],
            user=st.secrets["postgres"]["user"],
            password=st.secrets["postgres"]["password"],
            host=st.secrets["postgres"]["host"],
            port=st.secrets["postgres"]["port"]
        )
        st.success("✅ Database Connection Established Successfully!")
        return conn
    except Exception as e:
        st.error(f"❌ Database Connection Failed: {str(e)}")
        return None

def fetch_standard_columns(conn):
    """Fetch standard column names from the database."""
    try:
        query = "SELECT report_columns FROM RevReport"
        return pd.read_sql(query, conn)['report_columns'].tolist()
    except Exception as e:
        st.error(f"Error fetching standard columns: {str(e)}")
        return []

def find_data_start(df: pd.DataFrame, standard_columns: list) -> int:
    """Identify where the data begins based on standard column names."""
    for idx, row in df.iterrows():
        if any(any(possible_col.lower() in str(cell).lower() 
                   for possible_col in standard_columns) 
               for cell in row):
            return idx
    return 0

def map_columns(processed_df, revreport_df):
    """
    Map columns of the processed DataFrame using RevReport data with recursive mapping 
    and persistent column mapping.
    
    Args:
        processed_df (pd.DataFrame): Input DataFrame to map columns for
        revreport_df (pd.DataFrame): DataFrame containing report column mapping information
    
    Returns:
        tuple: Processed DataFrame, list of unmatched columns, and column name mapping
    """
    # Initialize or retrieve persistent column mapping from session state
    if 'column_mapping' not in st.session_state:
        st.session_state.column_mapping = pd.DataFrame(columns=['Original Column', 'Mapped Column'])
    
    # Retrieve existing mapping from session state for quick reference
    existing_mapping = {row['Original Column']: row['Mapped Column'] 
                        for _, row in st.session_state.column_mapping.iterrows()}
    
    report_columns = revreport_df['report_columns'].str.lower().tolist()
    required_columns = revreport_df['required_columns'].tolist()
    
    unmatched_columns = []
    column_name_mapping = {}
    matched_required_columns = []

    # First pass: Map columns based on exact matches and existing mapping
    for col in processed_df.columns:
        # Check for direct match in existing mapping
        if col in existing_mapping:
            new_col_name = existing_mapping[col]
            column_name_mapping[col] = new_col_name
            matched_required_columns.append(new_col_name)
            continue
        
        # Check for lowercase match in report columns
        if col.lower() in report_columns:
            idx = report_columns.index(col.lower())
            new_col_name = required_columns[idx]
            column_name_mapping[col] = new_col_name
            matched_required_columns.append(new_col_name)
            continue
        
        # If no match found, mark as unmatched
        unmatched_columns.append(col)
    
    # Apply matched column names
    processed_df.rename(columns=column_name_mapping, inplace=True)
    
    # Filter out already matched required columns
    remaining_required_columns = [col for col in required_columns if col not in matched_required_columns]
    
    # Recursive mapping for unmatched columns
    def recursive_column_mapping(unmatched_cols):
        # If no unmatched columns left, return
        if not unmatched_cols:
            return processed_df
        
        # Take the first unmatched column
        unmatched_col = unmatched_cols[0]
        
        # Check if this column is already in the existing mapping
        if unmatched_col in existing_mapping:
            new_name = existing_mapping[unmatched_col]
        else:
            # If no existing mapping, use a dropdown
            new_name = st.selectbox(
                f"Select a name for unmatched column: {unmatched_col}",
                options=[""] + remaining_required_columns,
                key=f"mapping_{unmatched_col}"
            )
        
        # If a new name is selected
        if new_name:
            # Rename the column
            processed_df.rename(columns={unmatched_col: new_name}, inplace=True)
            
            # Update session state mapping
            new_row = pd.DataFrame({
                'Original Column': [unmatched_col],
                'Mapped Column': [new_name]
            })
            st.session_state.column_mapping = pd.concat([
                st.session_state.column_mapping, 
                new_row
            ], ignore_index=True)
            
            # Remove the mapped column from remaining columns
            if new_name in remaining_required_columns:
                remaining_required_columns.remove(new_name)
            
            # Recursively map the next unmatched column
            return recursive_column_mapping(unmatched_cols[1:])
        
        return processed_df
    
    # Call recursive mapping
    processed_df = recursive_column_mapping(unmatched_columns)
    
    return processed_df, unmatched_columns, column_name_mapping

def save_to_db(conn):
    """Save the column mappings to the database."""
    try:
        with conn.cursor() as cur:
            # Get the column mapping from session state
            column_mapping = st.session_state.column_mapping
            
            # Track if any new columns were added
            new_columns_added = False
            
            # Insert or update mappings
            for _, row in column_mapping.iterrows():
                original_col = row['Original Column']
                mapped_col = row['Mapped Column']
                
                # Insert into RevReport
                cur.execute("""
                    INSERT INTO RevReport (report_columns, required_columns) 
                    VALUES (%s, %s) 
                    ON CONFLICT (report_columns) DO UPDATE 
                    SET required_columns = EXCLUDED.required_columns
                    RETURNING (xmax = 0) as inserted
                """, (original_col, mapped_col))
                
                # Check if a new row was inserted
                result = cur.fetchone()
                if result and result[0]:
                    new_columns_added = True
            
            conn.commit()
            
            # Provide appropriate feedback
            if new_columns_added:
                st.success("✅ New column mappings have been added to the database.")
            else:
                st.info("ℹ️ No new column mappings were added. Existing mappings were updated.")
    except Exception as e:
        st.error(f"Error saving to the database: {str(e)}")

def create_streamlit_app():
    st.title("Report Processing Application")
    
    # Automatically establish database connection
    conn = create_db_connection()
    if conn:
        # Fetch standard columns from the database
        standard_columns = fetch_standard_columns(conn)
    else:
        st.error("Failed to establish database connection.")
        return  # Stop further execution if connection fails

    # Upload and process Excel report
    uploaded_file = st.file_uploader("Upload your Excel report", type=['xlsx', 'xls'])
    
    if uploaded_file is not None:
        try:
            # Reset session state column mapping for new file
            st.session_state.column_mapping = pd.DataFrame(columns=['Original Column', 'Mapped Column'])
            
            df = pd.read_excel(uploaded_file)
            start_row = find_data_start(df, standard_columns)
            if start_row > 0:
                df = df.iloc[start_row:].reset_index(drop=True)
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)

            st.subheader("Processed DataFrame")
            st.dataframe(df.head())
            
            # Fetch and apply RevReport data
            revreport_df = pd.read_sql("SELECT report_columns, required_columns FROM RevReport", conn)
            if not revreport_df.empty:
                st.write("Matching columns with RevReport...")
                df, unmatched_columns, column_mapping = map_columns(df, revreport_df)
                st.subheader("Final DataFrame")
                st.dataframe(df.head())
                
                # Submit button to save column mappings
                if st.button("Submit Mapping"):
                    save_to_db(conn)
                
                # Optional: Display current mapping
               # st.subheader("Current Column Mapping")
               # st.dataframe(st.session_state.column_mapping)
            else:
                st.warning("No data found in RevReport table.")
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    create_streamlit_app()