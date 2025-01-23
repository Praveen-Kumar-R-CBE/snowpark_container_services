import streamlit as st
import pandas as pd
import json
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame
import utils as u
from datetime import datetime
import os

# connect to Snowflake
with open('creds-sample.json') as f:
    connection_parameters = json.load(f)  
session = Session.builder.configs(connection_parameters).create()

st.sidebar.title("Select Stage Location")
stage_options = {
    "Acturials": "@VOLUMES",
    "CEC": "@SPECS",
}

selected_stage = st.sidebar.selectbox("Choose where to save the file:", list(stage_options.keys()))

def loadInferAndPersist(file) -> snowpark.DataFrame:
    file_df = pd.read_csv(file)
    snowparkDf=session.write_pandas(file_df,file.name,auto_create_table = True, overwrite=True)
    return snowparkDf

def loadToStage(stage_file) :
    sf_stage_name:str = stage_options[selected_stage]
#TRY TO PUT FILE
    try:
        #PUT THE FILE
        put_results = session.file.put(
            local_file_name=stage_file,
            stage_location=sf_stage_name,
            overwrite=False,
            auto_compress=False)
        #PRINT THE RESULTS
        for r in put_results:
            str_output = ("File {src}: {stat}").format(src=r.source,stat=r.status)
            res = str_output        
    except Exception as e:
        #PRINT THE ERROR
        res = e
    return res

st.header("Everly Data Upload Portal")

file = st.file_uploader("Drop your CSV here", type={"csv"})
if file is not None:
    st.write(f"Uploaded file: {file.name}")
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # Save the file to a local path
    f_name = f"{current_time}_{file.name}"
    save_path = f"./{f_name}" 
    with open(save_path, "wb") as f:
        f.write(file.getbuffer())
    
    lst = loadToStage(f_name)

    os.remove(save_path)

    st.success(f"File staged successfully : {lst}")


query = f"SELECT RELATIVE_PATH AS FILE_NAME,SIZE,LAST_MODIFIED,MD5 FROM DIRECTORY({stage_options[selected_stage]} )" 

# Button to execute the query
if st.button("Fetch Stage Files"):
    try:
        
        # Execute the query using Snowpark
        st.info("Fetching Files from Stage...")
        result_df = session.sql(query).to_pandas()

        # Display results
        if not result_df.empty:
            # st.success("Query executed successfully!")
            st.write("Results:")
            st.dataframe(result_df)
        else:
            st.warning("The query executed successfully but returned no results.")
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        session.close()

file_delete = st.text_input("Enter a file name to delete from stage:")
del_query = f"REMOVE {stage_options[selected_stage]}/{file_delete} " 
if st.button("Delete Staged File"):
    try:
        
        # Execute the query using Snowpark
        st.info("Deleting Files from Stage...")
        result_df = session.sql(del_query).collect()
        df = pd.DataFrame([row.as_dict() for row in result_df])
        
        # Display results
        if not df.empty:
            # st.success("Query executed successfully!")
            # st.write("Results:")
            # df = result_df.DataFrame([row.as_dict() for row in result])
            st.write(df)
        else:
            st.warning("The query executed successfully but returned no results.")
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        session.close()