# Import python packages
import streamlit as st
import json
import time
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

## This application reads the content and shows it in a pandas dataframe then overwrites the table in snowflake
# connect to Snowflake
with open('creds-sample.json') as f:
    connection_parameters = json.load(f)  
session = Session.builder.configs(connection_parameters).create()


st.set_page_config(layout="wide")

def get_databases():
    databases = session.sql("SHOW DATABASES").collect()
    database_list = [row[1] for row in databases if row[1] not in ["SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"]]
    return database_list

def get_schemas(database):
    schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
    return [row[1] for row in schemas if row[1] not in ["INFORMATION_SCHEMA"]]

def get_tables(database, schema):
    tables = session.sql(f"SHOW TABLES IN SCHEMA {database}.{schema}").collect()
    return [row[1] for row in tables]

## StreamLit Main Page UI

st.title("❄️ Snowflake :blue[Data Editor]")

# Get the current credentialswith col1:
# session = get_active_session()

col1, col2, col3 = st.columns(3)

with col1:
    db_option = st.selectbox("Select Database", get_databases(), key="database")

with col2:
    sch_option = st.selectbox("Select Schema", get_schemas(db_option), key="schema")

with col3:
    tbl_option = st.selectbox("Select Table", get_tables(db_option, sch_option), key="table")

st.divider()

if db_option and sch_option and tbl_option:
    original_dataset = session.table(f"{db_option}.{sch_option}.{tbl_option}").to_pandas()

    with st.form("data_editor_form"):
        edited_data = st.data_editor(original_dataset, use_container_width=True, hide_index=True, num_rows="dynamic")
        submit_button = st.form_submit_button("Submit")

    if submit_button:
        try:
            session.write_pandas(
                df=edited_data,
                table_name=tbl_option,
                overwrite=True,
                quote_identifiers=False,
                database=db_option,
                schema=sch_option
            )
            st.success(f"Table {tbl_option} has been updated successfully")
            time.sleep(5)
        except:
            st.warning("Error updating table")
            st.experimental_rerun()

else:
    st.info("Kindly choose all the valid parameters from the dropdown", icon="ℹ")