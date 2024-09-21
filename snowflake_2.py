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
        "ACCOUNT" : "fogauss-tk79561",
        "USER": "manjub28",
        "PASSWORD": "Reddy@28",
        "ROLE": "ACCOUNTADMIN",
        "DATABASE": "SNOWFLAKE_SAMPLE_DATA",
        "SCHEMA" : "TPCH_SF1",
        "WAREHOUSE": "SNOWPARK_ETL_WH"
        }
    return Session.builder.configs(connection_parameters).create()



def ingest_in_sales(session) -> None:
    session.sql(" \
            copy into sales_dwh.source.in_sales_order from (\
                select \
                sales_dwh.source.in_sales_seq.nextval, \
                t.$1:: text as order_id, \
                t.$2:: text as customer_name, \
                t.$3:: text as mobile_key, \
                t.$4:: number as order_quantity, \
                t.$5:: number as unit_price, \
                t.$6:: number as order_value, \
                t.$7:: text as promotion_code , \
                t.$8:: number(10,2) as final_order_amount, \
                t.$9:: number(10,2) as tax_amount, \
                t.$10:: date as order_dt, \
                t.$11:: text as payment_status, \
                t.$12:: text as shipping_status, \
                t.$13:: text as payment_method, \
                t.$14:: text as payment_provider, \
                t.$15:: text as mobile, \
                t.$16:: text as shipping_address, \
                metadata$filename as stg_file_name, \
                metadata$file_row_number as stg_row_numer, \
                metadata$file_last_modified as stg_last_modified\
                from \
                @sales_dwh.source.my_internal_stg/source=IN/format=csv/ \
                ( \
                    file_format => 'sales_dwh.common.my_csv_format' \
                )t ) on_error = 'Continue' \
                ").collect()

def ingest_us_sales(session) -> None:
    session.sql('\
                copy into sales_dwh.source.us_sales_order        \
                from                                             \
                (                                                \
                    select                                       \
                    sales_dwh.source.us_sales_seq.nextval,  \
                    $1:"Order ID":: text as order_id,            \
                    $1:"Customer Name":: text as customer_name,   \
                    $1:"Mobile Model":: text as mobile_key,      \
                    to_number($1:"Quantity") as quantity,      \
                    to_number($1:"Price per Unit") as unit_price, \
                    to_decimal($1:"Total Price") as order_value, \
                    $1:"Promotion Code":: text as promotion_code, \
                    $1:"Order Amount":: number(10,2) as final_order_amount,\
                    to_decimal($1:"Tax") as tax_amount, \
                    $1:"Order Date":: date as order_dt, \
                    $1:"Payment Status":: text as payment_status, \
                    $1:"Shipping Status":: text as shipping_status,\
                    $1:"Payment Method":: text as payment_method, \
                    $1:"Payment Provider":: text as payment_provider, \
                    $1:"Phone":: text as phone, \
                    $1:"Delivery Address":: text as shipping_address, \
                    metadata$filename as stg_file_name,           \
                    metadata$file_row_number as stg_row_numer, \
                    metadata$file_last_modified as stg_last_modified\
                    from                                   \
                        @sales_dwh.source.my_internal_stg/source=US/format=parquet/ \
                        (file_format => sales_dwh.common.my_parquet_format) \
                        ) \
                '
                ).collect()

def ingest_fr_sales(session) -> None:
    session.sql('\
                copy into sales_dwh.source.fr_sales_order from(\
                select sales_dwh.source.fr_sales_seq.nextval,\
                $1:"Order ID":: text as order_id,\
                $1:"Customer Name":: text as customer_name,\
                $1:"Mobile Model":: text as mobile_key, \
                to_number($1:"Quantity") as order_quantity, \
                to_number($1:"Price per Unit") as unit_price, \
                to_decimal($1:"Total Price") as order_value,\
                $1:"Promotion Code":: text as promotion_code,\
                $1:"Order Amount":: number(10,2) as final_order_amount,\
                to_decimal($1:"Tax") as tax_amount,\
                $1:"Order Date":: date as order_dt, \
                $1:"Payment Status":: text as payment_status, \
                $1:"Shipping Status":: text as shipping_status, \
                $1:"Payment Method":: text as payment_method,\
                $1:"Payment Provider":: text as payment_provider, \
                $1:"Phone":: text as phone,\
                $1:"Delivery Address":: text as shipping_address, \
                metadata$filename as stg_file_name, \
                metadata$file_row_number as stg_row_numer,\
                metadata$file_last_modified as stg_last_modified\
                from @sales_dwh.source.my_internal_stg/source=FR/format=json/ \
                (file_format => sales_dwh.common.my_json_format) \
                ) \
                ').collect()
    
def main():
    session = get_snowpark_session()
    print("<India Sales Order> Before copy")
    session.sql("select count(*) from sales_dwh.source.in_sales_order").show()
    ingest_in_sales(session)
    print("<India Sales Order> After copy")
    session.sql("select count (*) from sales_dwh.source.in_sales_order").show()

    print("<US Sales Order> Before copy")
    session.sql("select count (*) from sales_dwh.source.us_sales_order").show()
    ingest_us_sales(session)
    print ("<US Sales Order> After copy")
    session.sql("select count (*) from sales_dwh.source.us_sales_order").show()

    print("<FR Sales Order> Before copy")
    session.sql("select count (*) from sales_dwh.source.fr_sales_order").show()
    ingest_fr_sales (session)
    print("<FR Sales Order> After copy")
    session.sql("select count (*) from sales_dwh.source.fr_sales_order").show()


if __name__ == "__main__":
    main()



    
