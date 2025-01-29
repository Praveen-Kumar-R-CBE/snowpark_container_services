import streamlit as st
import pandas as pd

# Convert your dataframe into pandas
df = pd.read_csv("data/employees.csv", header=0).convert_dtypes()
# df = df_data.to_pandas()

# Route it through the streamlit download button
def convert_df(df):
 csv = df.to_csv().encode("utf-8")
 st.download_button(
 label="Click to Download data as `.csv`",
 data=csv,
 mime="text/csv",
 )

convert_df(df)