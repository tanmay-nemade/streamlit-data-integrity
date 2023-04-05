import os
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from data_diff import connect_to_table, diff_tables
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
    sfDB = config[account]['sfDB']
    sfSchema = config[account]['sfSchema']
    sfWarehouse = config[account]['sfWarehouse']

    #dictionary with names and values of connection parameters
    conn = {"driver": "snowflake",
            "account": sfAccount,
            "user": sfUser,
            "password": sfPass,
            "role": sfRole,
            "warehouse": sfWarehouse,
            "database": sfDB,
            "schema": sfSchema}
    return conn

def session_builder(conn):
    session = Session.builder.configs(conn).create()
    return session

def db_list(session):
    dbs = session.sql("show databases ;").collect()
    #db_list = dbs.filter(col('name') != 'SNOWFLAKE')
    db_list = [list(row.asDict().values())[1] for row in dbs]
    return db_list


def schemas_list(chosen_db, session):
    # .table() tells us which table we want to select
    # col() refers to a column
    # .select() allows us to chose which column(s) we want
    # .filter() allows us to filter on coniditions
    # .distinct() means removing duplicates
    
    session.sql('use database :chosen_db;')
    fq_schema_name = chosen_db+'.information_schema.tables'
    

    schemas = session.table(fq_schema_name)\
            .select(col("table_schema"),col("table_catalog"),col("table_type"))\
            .filter(col('table_schema') != 'INFORMATION_SCHEMA')\
            .filter(col('table_type') == 'BASE TABLE')\
            .distinct()
            
    schemas_list = schemas.collect()
    # The above function returns a list of row objects
    # The below turns iterates over the list of rows
    # and converts each row into a dict, then a list, and extracts
    # the first value
    schemas_list = [list(row.asDict().values())[0] for row in schemas_list]
    return schemas_list

def tables_list(chosen_db, chosen_schema, session):

    fq_schema_name = chosen_db+'.information_schema.tables'
    #tables = session.table('sf_demo.information_schema.tables')\
    tables = session.table(fq_schema_name)\
        .select(col('table_name'), col('table_schema'), col('table_type') )\
        .filter(col('table_schema') == chosen_schema)\
        .filter(col('table_type') == 'BASE TABLE')\
        .sort('table_name')
    tables_list = tables.collect()
    tables_list = [list(row.asDict().values())[0] for row in tables_list]
    return tables_list

acc_select = st.selectbox('Choose account',(accounts))
conn = sfAccount_selector(acc_select)
session = session_builder(conn)

table1, table2 = st.columns(2)

with table1:
    st.write('Data for Destination Table')
    database = db_list(session)
    db_select1 = st.selectbox('Choose Destination Database',(database))
    conn["database"] = db_select1
    schemas = schemas_list(db_select1, session)
    sc_select1 = st.selectbox('Choose Destination Schema',(schemas))
    conn["schema"] = sc_select1
    tables = tables_list(db_select1,sc_select1, session)
    tb_select1 = st.selectbox('Choose Destination Table',(tables))
    conn["table"] = tb_select1
    snowflake_table1 = connect_to_table(conn, tb_select1,'Name')

    

with table2:
    st.write('Data for Source Table')
    database = db_list(session)
    db_select2 = st.selectbox('Choose Source Database',(database))
    conn["database"] = db_select2
    schemas = schemas_list(db_select2, session)
    sc_select2 = st.selectbox('Choose Source Schema',(schemas))
    conn["schema"] = sc_select2
    tables = tables_list(db_select2,sc_select2, session)
    tb_select2 = st.selectbox('Choose Source table',(tables))
    conn["table"] = tb_select2
    snowflake_table2 = connect_to_table(conn, tb_select2,'Name')

st.write('Different rows are:')
for different_row in diff_tables(snowflake_table1, snowflake_table2):
    plus_or_minus, columns = different_row
    if plus_or_minus == '-':
        query = '''select * from {}.{}.{} where "Name" = '{}';'''.format(db_select1,sc_select1,tb_select1,columns[0])
        data = session.sql(query).collect()
        st.write('In Source but not in Destination')
        st.table(data)
    else:
        query = '''select * from {}.{}.{} where "Name" = '{}';'''.format(db_select2,sc_select2,tb_select2,columns[0])
        data = session.sql(query).collect()
        st.write('In Destination but not in Source')
        st.table(data)
