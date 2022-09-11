import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

# Extract task
@task()
def get_src_tables():

    # Interact with the sql server
    hook = MsSqlHook(mssql_conn_id = 'sqlserver')

    # Get table names
    sql = """
        SELECT 
        t.name as table_name
        FROM
        sys.tables t
        WHERE
        t.name IN ('DimProduct','DimProductSubcategory','DimProductCategory')
    """

    # Put it in a dataframe
    df = hook.get_pandas_df(sql)
    print(df)

    # Convert to dictionary
    tbl_dict = df.to_dict('dict')

    return tbl_dict

# get raw data into postgres
@task()
def load_src_data(tabl_dict: dict):

    # Connect to postgresql
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    
    # Array to store table names
    all_tbl_name = []

    start_time = time.time()

    # Access table_name in dict
    for k, v in tabl_dict['table_name'].items():
        all_tbl_name.append(v)
        rows_imported = 0
        sql = f'SELECT * FROM {v}'

        # Query from sql server
        hook = MsSqlHook(mssql_conn_id='sql_server')
        df = hook.get_pandas_df(sql)
        # Success message
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}...for table {v}')

        # Persist data
        df.to_sql(f'src_{v}', engine, if_exist = 'replace', index = False)
        # Success message
        rows_imported += len(df)
        print(f'Done, {str(round(time.time() - start_time, 2))} total seconds elapsed')

    print('Data imported successfully')
    return all_tbl_name

# Transform source DimProduct table to staging table
@task()
def transform_srcProduct():

    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('SELECT * FROM public."src_DimProduct"', engine)

    # Drop unwanted columns
    revised = pdf[['ProductKey', 'ProductAlternateKey', 'ProductSubcategoryKey','WeightUnitMeasureCode', 
    'SizeUnitMeasureCode','EnglishProductName', 'StandardCost','FinishedGoodsFlag', 'Color', 'SafetyStockLevel', 
    'ReorderPoint','ListPrice', 'Size','SizeRange', 'Weight','DaysToManufacture','ProductLine', 'DealerPrice', 
    'Class', 'Style', 'ModelName', 'EnglishDescription','StartDate','EndDate', 'Status']]

    # Replace nulls with zeros
    revised['WeightUnitMeasureCode'].fillna('0', inplace=True)
    revised['ProductSubcategoryKey'].fillna('0', inplace=True)
    revised['SizeUnitMeasureCode'].fillna('0', inplace=True)
    revised['StandardCost'].fillna('0', inplace=True)
    revised['ListPrice'].fillna('0', inplace=True)
    revised['ProductLine'].fillna('NA', inplace=True)
    revised['Class'].fillna('NA', inplace=True)
    revised['Style'].fillna('NA', inplace=True)
    revised['Size'].fillna('NA', inplace=True)
    revised['ModelName'].fillna('NA', inplace=True)
    revised['EnglishDescription'].fillna('NA', inplace=True)
    revised['DealerPrice'].fillna('0', inplace=True)
    revised['Weight'].fillna('0', inplace=True)

    # Rename columns beginning with 'English'
    revised = revised.rename(columns={"EnglishDescription": "Description", 
    "EnglishProductName":"ProductName"})

    # Save into a staging table
    revised.to_sql(f'stg_DimProduct', engine, if_exists='replace', index=False)
    
    return {'table(s) processed ': 'Data imported succesfully'}

# Transform source DimProductSubcategory table to staging table
@task()
def transform_srcProductSubcategory():

    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('SELECT * FROM public."src_DimProductSubcategory" ', engine)

    # Drop unwanted columns
    revised = pdf[['ProductSubcategoryKey','EnglishProductSubcategoryName', 'ProductSubcategoryAlternateKey',
    'EnglishProductSubcategoryName', 'ProductCategoryKey']]

    # Rename columns beginning with 'English'
    revised = revised.rename(columns={"EnglishProductSubcategoryName": "ProductSubcategoryName"})

    # Save into a staging table
    revised.to_sql(f'stg_DimProductSubcategory', engine, if_exists='replace', index=False)

    return {"table(s) processed ": "Data imported successful"}

# Transform source DimProductCategory table to staging table
@task()
def transform_srcProductCategory():

    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('SELECT * FROM public."src_DimProductCategory" ', engine)

    # Drop unwanted columns
    revised = pdf[['ProductCategoryKey', 'ProductCategoryAlternateKey','EnglishProductCategoryName']]

    # Rename columns beginning with 'English'
    revised = revised.rename(columns={"EnglishProductCategoryName": "ProductCategoryName"})

    # Save into a staging table
    revised.to_sql(f'stg_DimProductCategory', engine, if_exists='replace', index=False)

    return {"table(s) processed ": "Data imported successful"}

# Final product model
@task()
def prdProduct_model():

    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    p  = pd.read_sql_query('SELECT * FROM public."stg_DimProduct"', engine)
    ps = pd.read_sql_query('SELECT * FROM public."stg_DimProductSubcategory"', engine)
    pc = pd.read_sql_query('SELECT * FROM public."stg_DimProductCategory"', engine)

    # fix datatype mismatch
    p['ProductSubcategoryKey'] = p.ProductSubcategoryKey.astype(float)
    p['ProductSubcategoryKey'] = p.ProductSubcategoryKey.astype(int)

    # Merge the three tables
    merged = p.merge(ps, on = 'ProductSubcategoryKey').merge(pc, on = 'ProductCategoryKey')
    merged.to_sql(f'prd_DimProductCategory', engine, if_exists='replace', index=False)

    return {"table(s) processed ": "Data imported successful"}

# Declare DAG
with DAG(dag_id = 'product_etl_dag', schedule_interval = '0 9 * * *', start_date = datetime(2022, 7, 8), 
catchup = False, tags = ['product_model']) as dag:
    
    # Extract
    with TaskGroup("extract_dimProudcts_load", tooltip="Extract and load source data") as extract_load_src:
        src_product_tbls = get_src_tables() 
        load_dimProducts = load_src_data(src_product_tbls)

        # Order
        src_product_tbls >> load_dimProducts

    # Transform
    with TaskGroup("transform_src_product", tooltip="Transform and stage data") as transform_src_product:
        transform_srcProduct = transform_srcProduct()
        transform_srcProductSubcategory = transform_srcProductSubcategory()
        transform_srcProductCategory = transform_srcProductCategory()

        # Order (parallel)
        [transform_srcProduct, transform_srcProductSubcategory, transform_srcProductCategory]

    # Load
    with TaskGroup("load_product_model", tooltip = "Final product model") as load_product_model: 
        prd_Product_model = prdProduct_model()

        # Order
        prd_Product_model
    
    # Overall order
    extract_load_src >> transform_src_product >> load_product_model