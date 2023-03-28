import os
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import streamlit as st

st.set_page_config(
    layout="wide",
    page_title="Streamlit Data Integrity"
)

st.title('Streamlit Data Integrity App')

config = configparser.ConfigParser()
config.read('config.ini')
sections = config.sections()
accounts = []
for section in sections:
    accounts.append(section)

def sfAccount_selector(account):
    #setup config.ini read rules
    sfAccount = config[account]['sfAccount']
    sfUser = config[account]['sfUser']
    sfPass = config[account]['sfPass']
    sfRole = config[account]['sfRole']
    #sfDB = config[account]['sfDB']
    #sfSchema = config[account]['sfSchema']
    sfWarehouse = config[account]['sfWarehouse']

    #dictionary with names and values of connection parameters
    conn = {"account": sfAccount,
            "user": sfUser,
            "password": sfPass,
            "role": sfRole,
            "warehouse": sfWarehouse}
            #"database": sfDB,
            #"schema": sfSchema}

    #Create a session using the connection parameters
    session = Session.builder.configs(conn).create()
    return session