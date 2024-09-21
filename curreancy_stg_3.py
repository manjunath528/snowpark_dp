from snowflake. snowpark import Session
import sys 
import logging
from snowflake. snowpark import Session, DataFrame
from snowflake. snowpark. types import StructType, StringType, StructField, DateType, FloatType
from snowflake. snowpark. functions import col, lit, row_number, rank 
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
        "DATABASE": "SNOWFLAKE_SAMPLE_DATA",
        "SCHEMA" : "TPCH_SF1",
        "WAREHOUSE": "SNOWPARK_ETL_WH"
        }
    return Session.builder.configs(connection_parameters).create()

def main():
    session = get_snowpark_session()
    eur_in_data = '/Users/manjunathreddy/Desktop/exchange_files/EUR_INR_Historical_Data.csv'
    eur_us_data = '/Users/manjunathreddy/Desktop/exchange_files/EUR_USD_Historical_Data.csv'
    local_in_path = "/Users/manjunathreddy/Desktop/exchange_files/final/eur_inr.csv"
    local_us_path = "/Users/manjunathreddy/Desktop/exchange_files/final/eur_usd.csv"

    df1 = pd.read_csv(eur_in_data,skiprows=1)
    df1.to_csv(local_in_path, index=False)

    df2 = pd.read_csv(eur_us_data,skiprows=1)
    df2.to_csv(local_us_path, index=False)


    local_stg_location = "/Users/manjunathreddy/Desktop/exchange_files/final/*.csv"

    stage_location = '@sales_dwh.source.my_exg_stg'


    session.file.put(local_stg_location, stage_location, auto_compress=False)


if __name__ == '__main__':
    main()



