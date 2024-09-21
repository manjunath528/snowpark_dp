from snowflake. snowpark import Session
import sys 
import logging
from snowflake. snowpark import Session, DataFrame
from snowflake. snowpark. types import StructType, StringType, StructField
from snowflake. snowpark. functions import col, lit, row_number, rank 
from snowflake.snowpark import Window

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

def filter_dataset(df, column_name, filter_criterian) -> DataFrame:
    return_df = df.filter(col(column_name) == filter_criterian)
    return return_df

def main() :
#get the session object and get dataframe
    session = get_snowpark_session()
    sales_df = session.sql("select * from sales_dwh.source.fr_sales_order")# apply filter to select only paid and delivered sale orders
    # select * from in_sales_order where PAYMENT_STATUS = 'Paid' and SHIPPING_S*
    paid_sales_df = filter_dataset(sales_df, 'PAYMENT_STATUS', 'Paid' )
    shipped_sales_df = filter_dataset(paid_sales_df, 'SHIPPING_STATUS','Delivered')
    # adding country and region to the data frame
    # select *, 'IN' as Country, 'APAC' as Region from in_sales_order where PAYN
    country_sales_df = shipped_sales_df.with_column('Country',lit('FR')).with_column('Region', lit('EU'))
    forex_df = session.sql("select * from sales_dwh.source.exg_rate")
    sales_with_forext_df = country_sales_df.join(forex_df,country_sales_df['order_dt']==forex_df['exchange_rate_dt'],join_type='outer')
    print(f'Sales {sales_with_forext_df.count()}')
    #sales_with_forext_df. show(2)
    unique_orders = sales_with_forext_df.with_column('order_rank', rank().over(Window.partitionBy(col("order_dt")).order_by(col('_metadata_last_modified').desc()))).filter(col("order_rank")==1).select(col('SALES_ORDER_KEY').alias('unique_sales_order_key'))
    final_sales_df = unique_orders.join(sales_with_forext_df,unique_orders['unique_sales_order_key']==sales_with_forext_df['SALES_ORDER_KEY'], join_type=' inner')
    print(f'Sales {final_sales_df.count()}')
    print(f'Sales {final_sales_df.columns}')
    final_sales_df = final_sales_df.select(
        col('SALES_ORDER_KEY'),
        col('ORDER_ID'),
        col('ORDER_DT'),
        col('CUSTOMER_NAME'),
        col('MOBILE_KEY'),
        col('Country'),
        col('Region'),
        col('ORDER_QUANTITY'),
        lit('EUR').alias('LOCAL_CURRENCY'),
        col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
        col('PROMOTION_CODE').alias('PROMOTION_CODE'),
        col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMT') ,
        col('TAX_AMOUNT').alias('local_tax_amt'),
        col('EU').alias('Exhchange_Rate'),
        (col('FINAL_ORDER_AMOUNT')/col('EU')).alias('EU_TOTAL_ORDER_AMT'),
        (col('TAX_AMOUNT')/col('EU')).alias('EU_TAX_AMT'),
        col('payment_status'),
        col('shipping_status'),
        col('payment_method'),
        col('payment_provider'),
        col('phone').alias('contact_no'),
        col('shipping_address')
    )

    new_table_name = "SALES_DWH.CURATED.FR_SALES_ORDER"

    final_sales_df.write.mode("append").save_as_table(new_table_name)
 

if __name__ == '__main__':
    main()

                        

