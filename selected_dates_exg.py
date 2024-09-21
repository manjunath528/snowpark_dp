from snowflake. snowpark import Session
import sys 
import logging
from snowflake. snowpark import Session, DataFrame
from snowflake. snowpark. types import StructType, StringType, StructField, DateType, FloatType
from snowflake. snowpark. functions import col, lit, row_number, rank , min as sf_min, max as sf_max
from snowflake.snowpark import Window
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructType as PySparkStructType, StructField as PySparkStructField, StringType as PySparkStringType, IntegerType as PySparkIntegerType, DoubleType as PySparkDoubleType, DateType as PySparkDateType


logging.basicConfig(stream=sys.stdout, level=logging. INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# snowpark session
def get_snowpark_session()->Session:
    connection_parameters = {
        "ACCOUNT" : "",
        "USER": "",
        "PASSWORD": "",
        "ROLE": "ACCOUNTADMIN",
        "DATABASE": "SALES_DWH",
        "SCHEMA" : "SOURCE",
        "WAREHOUSE": "SNOWPARK_ETL_WH"
        }
    return Session.builder.configs(connection_parameters).create()


def main():
    session = get_snowpark_session()
    in_sales_df = session.sql("select * from sales_dwh.source.in_sales_order")
    us_sales_df = session.sql("select * from sales_dwh.source.us_sales_order")
    fr_sales_df = session.sql("select * from sales_dwh.source.fr_sales_order")
    all_sales_df = in_sales_df.union(us_sales_df).union(fr_sales_df)


    start_date = all_sales_df.select(sf_min(col("order_dt")).alias("min_order_dt")).collect()[0].as_dict()["MIN_ORDER_DT"]
    end_date = all_sales_df.select(sf_max(col("order_dt")).alias("max_order_dt")).collect()[0].as_dict()["MAX_ORDER_DT"]
    print(f'start_date{start_date}')
    print(f'start_date{end_date}')
    date_range = pd.date_range(start=start_date, end=end_date,freq='D')
    column_names = ['EXG_DT']
    date_dim = pd.DataFrame(columns=column_names)
    date_dim['EXG_DT']=pd.to_datetime(date_range)
    date_dim['EXG_DT'] = date_dim["EXG_DT"].dt.date
    print(date_dim.count())
    temp_table ="snow_exg_df"
    session.write_pandas(date_dim,temp_table,auto_create_table = True,overwrite=True)
    
    existing_data = session.sql("select * from sales_dwh.source.exg_rate")
    modified_data = session.sql('select * from SALES_DWH.SOURCE."snow_exg_df"')
    final_data = modified_data.join(existing_data, modified_data['EXG_DT']==existing_data['EXCHANGE_RATE_DT'],join_type ='outer')
    print(final_data.show())


if __name__ == '__main__':
    main()