import datetime
from io import BytesIO
import streamlit as st
from sqlalchemy import create_engine, inspect, Table, Column, String, Float, BigInteger, Boolean, MetaData, Integer, \
    ForeignKey, Text, text, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import psycopg2
from annotated_text import annotated_text  # pip install st-annotated-text
import pandas as pd
import requests


# Function to drop any table
def drop_table(table):
    drop_query = text(f"DROP TABLE IF EXISTS {table} CASCADE")
    session.execute(drop_query)


# Fetch data from the table
def fetch_data_from_table(table):
    fetched_data = pd.read_sql_table(f"{table}", engine)
    return fetched_data


# Get schema of table
def get_schema(table_name):
    columns = inspector.get_columns(f"{table_name}")
    for column in columns:
        print(column['name'], column['type'])


def adsq_rename_and_modify_columns(df):
    for i in range(len(db['report_column_name'])):
        df.rename(columns={
            db['report_column_name'][i]: db['required_column_name'][i]
        }, inplace=True)
        columns=df.columns.tolist()
    try:
        df['platformName'] = 'Adsquare'
        df['brand'] = ''
        df['impressions'] = df['impressions'].apply(str)
        df['impressions'] = df['impressions'].apply(lambda x: int(float(x.replace(',', ''))))
        df['segId'] = df['segId'].apply(remove_s)
        try:
            for idx, val in enumerate(df['eMonth']):
                if not isinstance(val, datetime.datetime):
                    popup(val,idx,column='eMonth')
            df['eMonth'] = df['eMonth'].apply(adsq_time)
        except Exception as e:
            print(e)
        df['dsp'] = df['dspOriginal']
        df['agency'] = df['agencyOriginal']
        df['eDate'] = df.loc[:, 'eMonth'] + "-1"
        return columns, df, 0
    except KeyError as e:
        annotated_text(("Column ", "", "#bcbcbc"), (f"{e}", "", "#DC3545"),
                       " is missing in the excel file, check the appropriate column name from the excel file. Enter the column name in the text box below.")
        err = f"{e}"
        return columns, None, err
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def remove_s(attr):
    if not isinstance(attr, str):
        attr = str(attr)
    attr = attr.replace(' ', '').strip()
    if attr[0] == 's':
        attr = attr.replace('s', '').strip()
        return (attr)
    else:
        return (attr)


def adsq_time(time):
    return time.strftime('%Y-%m')


def parse_adsq(path):
    adsq = pd.read_excel(path, skiprows=10)
    adsq = adsq_rename_and_modify_columns(adsq)
    return adsq


# # This can be used for future because there may be a possiblity that the rows to be skipped may *change*
# def parse_adsq(path):
#     df=pd.read_excel(path)
#     # Find the first row where all columns are filled
#     start_row = df.dropna(how='any').index[0]
#     # Read the Excel file again, skipping the rows before the start_row
#     df = pd.read_excel(path, skiprows=range(start_row+1))
#     return df


# Establish connection
try:
    engine = create_engine("postgresql://postgres:QRFcLLtOw9os2HL7@impossibly-evolving-creeper.data-1.use1.tembo.io:5432/postgres")
except ConnectionError as e:
    print(f"ConnectionError: {e} - Check your database server, network connection, and connection string.")
    raise
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    raise

# Define metadata
metadata = MetaData()

# Create an inspector object
inspector = inspect(engine)

# report_columns table schema
report_columns = Table('report_columns', metadata,
                       Column('report_column_name', String, primary_key=True),
                       Column('required_column_name', String),
                       Column('insert_ts', DateTime(timezone=True), server_default=func.now()),
                       Column('excel_format', String),
                       extend_existing=True
                       )


def insert_data(newcol, reqcol, format):
    # Inserting dictionary values
    insert_statement = report_columns.insert().values(
        report_column_name=newcol,
        required_column_name=reqcol,
        excel_format=format
    )
    session.execute(insert_statement)
    session.commit()


def formatting_text(change):
    return change[1:len(change) - 1]


def get_file_format(reqcol, db):
    if reqcol in db['required_column_name'].values:
        return db.loc[db['required_column_name'] == reqcol, 'excel_format'].values[0]
    else:
        return None


def fetch_file_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching file: {e}")
        return None


def displaying_and_handling_file(file):
    try:
        columns, df, change = parse_adsq(file)
        if df is None:
            if change:
                change = formatting_text(change)
                format=get_file_format(change,db)
                option = st.selectbox(
                    "Select Column Name",
                    columns,
                )
                # st.write("You selected:", option)
                if st.button("Add"):
                    insert_data(option, change, format)
                    st.caption("Updated Successfully")
                    st.rerun()
                st.subheader("You may get some idea from here:")
                filtered_db = db[db['required_column_name'] == change]
                st.dataframe(data=filtered_db[['report_column_name','required_column_name']])
        else:
            st.write("File Fetched Successfully!")
            st.dataframe(data=df)
    except Exception as e:
        print(e)


# Create a session
Session = sessionmaker(bind=engine)
session = Session()

db = fetch_data_from_table('report_columns')


def reset_state():
    st.session_state.reset = True


@st.dialog("Found Inconsistency in Column Values")
def popup(val,idx,column):
    st.error(f"Value: '{val}' at index {idx} in the column '{column}' is not in the form of 'MMM-YY'. Change this and re-upload")
    st.session_state.reset = True



def st_fragment():
    file_url = st.text_input("File URL", placeholder="Enter the URL of the file")
    st.markdown("<h1 style='text-align: center;'>OR</h1>", unsafe_allow_html=True)
    uploaded_file = st.file_uploader("Upload File")
    if uploaded_file and not file_url:
        file_content = BytesIO(uploaded_file.read())
        displaying_and_handling_file(file_content)
    if not uploaded_file and file_url:
        file_content = fetch_file_from_url(file_url)
        displaying_and_handling_file(file_content)
    if uploaded_file and file_url:
        st.error("Please remove either the url or the uploaded file.")



# Streamlit UI
st.title("Revenue Report Uploader")

# print(change)


st_fragment()