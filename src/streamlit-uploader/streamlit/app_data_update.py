# Import python packages
import streamlit as st
import json
from streamlit import session_state as ss
import time
import snowflake.snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.set_page_config(layout="centered", page_title="Data Editor", page_icon="🧮")


# connect to Snowflake
with open('creds-sample.json') as f:
    connection_parameters = json.load(f)  
session = Session.builder.configs(connection_parameters).create()


tabname = ""

st.title("Snowflake Table Editor ❄️")

def get_dataset(tbname):
    #
    # load messages df
    #
    df = session.table(tbname).to_pandas()
    #
    # rebuild the index for the datafram
    #
    df.reset_index(inplace = True, drop = True)
    return df

#
# Get the shema and database
#
def get_tables():
    dbdf = session.sql("select current_database() as DB").to_pandas()
    dbname = dbdf['DB'][0]
    dbdf = session.sql("select current_schema() as CS").to_pandas()
    schema = dbdf['CS'][0]
    #
    # Get the list of tables for the database
    #
    sql = "SELECT table_name FROM " + dbname + ".information_schema.tables WHERE table_schema = '" + schema + "'"
    tables_df = session.sql(sql).to_pandas()
    return tables_df

#
# Process the columns for the updated record for the UPDATE statement
#
def process_cols(columns):
    i = 0
    stmt = ""
    for c in columns:
        if i == 0:
            stmt = "UPDATE " + tabname + " SET " + c + " = '" + columns[c] + "'"
            i = 5
        else:
            stmt = stmt + ", " + c + " = '" + columns[c] + "'"
    return stmt

#
# Get the columns and column values and build out the UPDATE statement
#
def select_cols (df, idx):
    first = True
    stmt = ""
    cols = list(df.columns.values)
    for col in cols:
        if first:
            stmt = " WHERE " + col + " = '" + str(df.iloc[idx][col]) + "'"
            first = False
        else:
            if str(df.iloc[idx][col]) == 'None':
                stmt = stmt + " AND " + col + " IS NULL "
            else:
                stmt = stmt + " AND " + col + " = '" + str(df.iloc[idx][col]) + "'"
    return stmt

#
# Get the columns and column values and build out the INSERT statement
#
def insert_cols(cols):
    first = True
    stmt = ""
    vals = ""
    for col in cols:
        if first:
            stmt = "INSERT INTO " + tabname + " ( " + col 
            vals = " VALUES ('" + str(cols[col]) + "'"
            first = False
        else:
            stmt = stmt + ", " + col 
            vals = vals + ", '" + str(cols[col]) + "'"
    return stmt + ") " + vals + ")"

#
# Get the columns / values for the DELETE statement
#
def delete_cols(idx, df):
    first = True
    stmt = ""
    cols = list(df.columns.values)
    for col in cols:
        if first:
            stmt = "DELETE FROM " + tabname + " WHERE " + col + " = '" + str(df.iloc[idx][col]) + "'"
            first = False
        else:
            if str(df.iloc[idx][col]) == 'None':
                stmt = stmt + " AND " + col + " IS NULL "
            else:
                stmt = stmt + " AND " + col + " = '" + str(df.iloc[idx][col]) + "'"
    return stmt

#
# Open up the editor
#

with st.form("table_selector_form"):
    tdf = get_tables()
    tabname = st.selectbox(
        "Select Table to Edit:",
        (tdf))
    table_submit_button = st.form_submit_button("Select ...")

if table_submit_button:
    st.write("Selected table: " + tabname)
    st.session_state['HAVE_TABLE'] = True
    st.rerun()

#
# Use the session state to ensure that a table has been selected before 
# loading the editor for the first time
#
if 'HAVE_TABLE' in st.session_state:  
    with st.form("data_editor_form"):

        #
        # Get the Table as a dataframe
        #
        dataset = get_dataset(tabname)
        st.caption("Edit the dataframe below")
        ss.edited = st.data_editor(dataset, use_container_width=True, num_rows="dynamic", key='ed')
        debug = st.checkbox('Debug.')
        submit_button = st.form_submit_button("Submit")

    if submit_button:
        idx = 0

        # Debug
        if debug:
            st.write("edited rows: ", ss.ed)   
            time.sleep(5)
        
        try:
            session = st.session_state.snowflake_connection

            # Uncomment for DEBUG
            #st.write("edited_rows: ", ss.ed["edited_rows"])
            #st.write("ss.ed struct: ", ss.ed)

            #
            # Process updated row(s)
            for rec in ss.ed["edited_rows"]:
                idx = int(rec)
                updt = process_cols(ss.ed["edited_rows"][rec])
                where = select_cols(dataset, idx)

                update_stmt = updt + " " + where

                # Debug
                if debug:
                    st.write("Update SQL: \n", update_stmt)
                
                result = session.sql(update_stmt).collect()
                st.write(result)

            #
            # Process newly inserted row(s)
            #
            for irec in ss.ed["added_rows"]:
                insert_stmt = insert_cols(irec)
                # Debug
                if debug:
                    st.write("Update SQL: \n", insert_stmt)
                
                result = session.sql(insert_stmt).collect()
                st.write(result)

            #
            # Process the deleted row(s)
            #
            for rec in ss.ed["deleted_rows"]:
                idx = int(rec)
                delete_stmt = delete_cols(idx, dataset)
                # Debug
                if debug:
                    st.write("Delete SQL: \n", delete_stmt)
                result = session.sql(delete_stmt).collect()
                st.write(result)


            time.sleep(2)

        except:
            st.warning("Error updating table")
            time.sleep(5)

 