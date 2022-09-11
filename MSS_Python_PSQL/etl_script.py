# For postgreSQL
from sqlalchemy import create_engine
import psycopg2
# For SQL server
import pyodbc 
import pandas as pd
# For username and password stored in system
import os
# For the pyodbc connection warning 
# (i.e Pandas likes a sqlalchemy connectable better)
import warnings
warnings.filterwarnings('ignore')

# Get password 
pwd = os.environ.get('PGPASS')
user_id = os.environ.get('PGUID')
# Get SQL server driver
driver = '{SQL Server Native Client 11.0}'
server = 'localhost'
database = 'AdventureWorksDW2019;'

# Extract data from SQL server
def extract():
    try:
        # SQL Server connection
        src_conn = pyodbc.connect(
            f"Driver={driver};"   
            f"Server={server};"
            f"Database={database};"
            f"UID={user_id};"
            f"PWD={pwd};"
        )
        # Allow sql commands in a database session
        src_cursor = src_conn.cursor()
        # Execute query 
        src_cursor.execute("""
            SELECT 
              t.name AS table_name
            FROM
              sys.tables t
            WHERE
              t.name IN ('DimProduct', 'DimProductSubcategory', 'DimProductCategory', 'DimSalesTerritory', 'FactInternetSales')
        """)
        # Get all tables (only 5 for simplicity sake)
        src_tables = src_cursor.fetchall()
        # query and load saved data to dataframe
        for table in src_tables:
            df = pd.read_sql_query(f'select * from {table[0]}', src_conn)
            load(df, table[0])
    
    except Exception as e:
        print('Data extraction error: ' + str(e))
    
    finally:
      src_conn.close()

# Load data into PostgreSQL
def load(df, table_name):
  try:
    rows_imported = 0
    # PostgreSQL connection
    engine = create_engine(f'postgresql://{user_id}:{pwd}@{server}:5432/AdventureWorks')
    print(f'Importing rows {rows_imported} to {rows_imported + len(df)}... for table {table_name}')
    
    # Save data to PostgreSQL
    df.to_sql(f'stg_{table_name}', engine, if_exists ='replace', index = False)
    rows_imported += len(df)
    print('Data imported succesfully')
    
  except Exception as e:
    print('Data loading error: ' + str(e))

try:
  # Call extract function
  extract()
except Exception as e:
  print('Error while extracting data: ' + str(e)) 