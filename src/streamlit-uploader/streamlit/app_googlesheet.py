import streamlit as st
import pandas as pd
import json
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame
from gspread_dataframe import set_with_dataframe
import utils as u
from datetime import datetime
import os
import gspread
from google.oauth2.service_account import Credentials

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("gcreds.json", scopes=scopes)
client = gspread.authorize(creds)

sheet_id = "170_iubyBAVfvUpB0BDbCSTjBIKkvWUlLwepMVIj3pGM" 



# print(values_list)


# # connect to Snowflake
with open('creds-sample.json') as f:
    connection_parameters = json.load(f)  
session = Session.builder.configs(connection_parameters).create()

def loadInferAndPersist(file) -> snowpark.DataFrame:
    file_df = pd.read_csv(file)
    snowparkDf=session.write_pandas(file_df,file.name,auto_create_table = True, overwrite=True)
    return snowparkDf

st.header("Everly Data Upload Portal")
file = st.file_uploader("Drop your CSV here", type={"csv"})
if file is not None:
    df= loadInferAndPersist(file)
    pandas_df = df.to_pandas()
    sheet = client.open_by_key(sheet_id)
    worksheet1 = sheet.worksheet('Sheet1')

    worksheet1.clear()
    set_with_dataframe(worksheet=worksheet1, dataframe=pandas_df, include_index=False,include_column_header=True, resize=True)
    # set_with_dataframe(worksheet=worksheet1,dataframe=df,include_index=False,include_column_header=False,row=worksheet1.row_count+1,resize=False)

    st.subheader("Great, your data has been uploaded to Snowflake!")
    
    # with st.expander("Technical information"):
        
        # u.describeSnowparkDF(df)
        # st.write("Data loaded to Snowflake:")
    st.dataframe(pandas_df.head(2))

