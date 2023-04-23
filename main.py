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

@st.cache_resource
def session_builder(conn):
    session = Session.builder.configs(conn).create()
    return session

@st.cache_resource 
def db_list(_session):
    dbs = _session.sql("show databases ;").collect()
    #db_list = dbs.filter(col('name') != 'SNOWFLAKE')
    db_list = [list(row.asDict().values())[1] for row in dbs]
    return db_list

@st.cache_resource
def schemas_list(chosen_db, _session):
    # .table() tells us which table we want to select
    # col() refers to a column
    # .select() allows us to chose which column(s) we want
    # .filter() allows us to filter on coniditions
    # .distinct() means removing duplicates
    
    _session.sql('use database :chosen_db;')
    fq_schema_name = chosen_db+'.information_schema.tables'
    

    schemas = _session.table(fq_schema_name)\
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

@st.cache_resource
def tables_list(chosen_db, chosen_schema, _session):

    fq_schema_name = chosen_db+'.information_schema.tables'
    #tables = session.table('sf_demo.information_schema.tables')\
    tables = _session.table(fq_schema_name)\
        .select(col('table_name'), col('table_schema'), col('table_type') )\
        .filter(col('table_schema') == chosen_schema)\
        .filter(col('table_type') == 'BASE TABLE')\
        .sort('table_name')
    tables_list = tables.collect()
    tables_list = [list(row.asDict().values())[0] for row in tables_list]
    return tables_list

@st.cache_resource(experimental_allow_widgets=True)
def table_choice(value, index):
    st.write('Data for {} Table'.format(value))
    database = db_list(session)
    db_select = st.selectbox('Choose {} Database'.format(value),(database), index=index)
    conn["database"] = db_select
    schemas = schemas_list(db_select, session)
    sc_select = st.selectbox('Choose {} Schema'.format(value),(schemas))
    conn["schema"] = sc_select
    tables = tables_list(db_select,sc_select, session)
    tb_select = st.selectbox('Choose {} table'.format(value),(tables))
    conn["table"] = tb_select
    key_id = st.text_input('Key Id for {} table'.format(value))
    snowflake_table = connect_to_table(conn, tb_select,key_id)
    return {'snowflake_table':snowflake_table, 'database':db_select, 'schema': sc_select, 'table':tb_select}



acc_select = st.selectbox('Choose account',(accounts))
conn = sfAccount_selector(acc_select)
session = session_builder(conn)

table1, table2 = st.columns(2)

with table1:
    source_data = table_choice('Source',0)
    
with table2:
    destination_data = table_choice('Destination',3)
    

if st.button('Compare'):
    st.write('Different rows are:')
    minus_df = []
    plus_df = []
    for different_row in diff_tables(source_data['snowflake_table'], destination_data['snowflake_table']):
        plus_or_minus, columns = different_row
        if plus_or_minus == '-':
            query = '''select * from {}.{}.{} where "Name" = '{}';'''.format(source_data['database'],source_data['schema'],source_data['table'],columns[0])
            data = session.sql(query).to_pandas()
            minus_df.append(data)
        else:
            query = '''select * from {}.{}.{} where "Name" = '{}';'''.format(destination_data['database'],destination_data['schema'],destination_data['table'],columns[0])
            data = session.sql(query).to_pandas()
            plus_df.append(data)
    st.write('In Source but not in Destination')
    st.table(pd.concat(minus_df, ignore_index=True))
    st.write('In Destination but not in Source')
    st.table(pd.concat(plus_df, ignore_index=True))
