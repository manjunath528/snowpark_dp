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

def currency_in_process(session) -> None:
     session.sql(' \
                 copy into sales_dwh.source.eur_ind_rate from (   \
                 select sales_dwh.source.in_exg_seq.nextval, \
                 $1:: date as exchange_rate_dt, \
                 $2:: number(10,4) as eu2inr \
                 from @sales_dwh.source.my_exg_stg/eur_inr.csv )'
                 ).collect()
     

def currency_us_process(session) -> None:
     session.sql(' \
                 copy into sales_dwh.source.eur_usd_rate from ( \
                 select sales_dwh.source.us_exg_seq.nextval, \
                 $1:: date as exchange_rate_dt, \
                 $2:: number(10,4) as eu2usd \
                 from @sales_dwh.source.my_exg_stg/eur_usd.csv)'
                ).collect()
     
def final_table(session) -> None:
     session.sql('\
                 insert into sales_dwh.source.exg_rate ( \
                 select sales_dwh.source.exg_seq.nextval,a.exchange_rate_dt, 1, b.eu2usd, a.eu2inr \
                 from sales_dwh.source.eur_ind_rate a \
                 inner join \
                 sales_dwh.source.eur_usd_rate b on a.exchange_rate_dt = b.exchange_rate_dt) \
                 ').collect()
     
if __name__ == "__main__":
     session = get_snowpark_session()

     print("<India Exchange Rate> Before copy")
     session.sql("select count(*) from sales_dwh.source.eur_ind_rate").show()
     currency_in_process(session)
     print("<India Exchange Rate> After copy")
     session.sql("select count(*) from sales_dwh.source.eur_ind_rate").show()
     print("<USA Exchange Rate> Before copy")
     session.sql("select count(*) from sales_dwh.source.eur_usd_rate").show()
     currency_us_process(session)
     print("<USA Exchange Rate> After copy")
     session.sql("select count(*) from sales_dwh.source.eur_usd_rate").show()
     print("<Exchange Rate> Before copy")
     session.sql("select count(*) from sales_dwh.source.exg_rate").show()
     final_table(session)
     print("< Exchange Rate> After copy")
     session.sql("select count(*) from sales_dwh.source.exg_rate").show()


