import streamlit as st

st.set_page_config(
     page_title="Anomaly Monitor",
     page_icon="📊",
     layout="wide",
     initial_sidebar_state="expanded"
)


import os
from streamlit_option_menu import option_menu


with st.sidebar:
    choose_side_opt = option_menu("Sample App Menu", ["Menu Item 1", "Menu Item 2", "Menu Item 3"],
                        icons=['house','tools','file-bar-graph-fill'],
                        menu_icon="cast", default_index=0,
                        styles={
                        "container": {"padding": "5!important", "background-color": "white","font-color": "#249dda"},
                        "icon": {"color": "#31c0e7", "font-size": "18px"}, 
                        "nav-link": {"font-size": "14px", "text-align": "left", "margin":"0px", "--hover-color": "white"},
                        "nav-link-selected": {"background-color": "7734f9"}
                        })

st.title('This is a Sample App');