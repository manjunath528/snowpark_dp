from snowflake. snowpark import Session 
import ipdb
import sys 
import logging
from snowflake. snowpark import Session, DataFrame
from snowflake. snowpark. types import StructType, StringType, StructField, DateType, FloatType
from snowflake. snowpark. functions import col, lit, row_number, rank, split, expr,cast, min, max
from snowflake.snowpark import Window
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructType as PySparkStructType, StructField as PySparkStructField, StringType as PySparkStringType, IntegerType as PySparkIntegerType, DoubleType as PySparkDoubleType, DateType as PySparkDateType


logging.basicConfig(stream=sys.stdout, level=logging. INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# snowpark session
def get_snowpark_session()->Session:
    connection_parameters = {
        "ACCOUNT" : "fogauss-tk79561",
        "USER": "manjub28",
        "PASSWORD": "Reddy@28",
        "ROLE": "ACCOUNTADMIN",
        "DATABASE": "SALES_DWH",
        "SCHEMA" : "CONSUMPTION",
        "WAREHOUSE": "SNOWPARK_ETL_WH"
        }
    return Session.builder.configs(connection_parameters).create()

def create_region_dim(all_sales_df, session) -> None:
    region_dim_df = all_sales_df.groupBy(col("Country"), col("Region")).count()
    region_dim_df.show(2)
    region_dim_df = region_dim_df.with_column("isActive", lit('Y'))
    region_dim_df = region_dim_df.selectExpr("SALES_DWH.CONSUMPTION.REGION_DIM_SEQ.nextval as region_id_pk","Country", "Region", "isActive")
    region_dim_df.show(5)
    existing_region_dim_df = session.sql("select Country, Region from sales_dwh.consumption.region_dim")
    region_dim_df = region_dim_df.join(existing_region_dim_df, region_dim_df['Country']==existing_region_dim_df['Country'], join_type='leftanti')
    region_dim_df.show(5)
    insert_cnt = int(region_dim_df.count())
    if insert_cnt>0:
        region_dim_df.write.save_as_table("sales_dwh.consumption.region_dim", mode="append")
        print ("save operation ran...")
    else:
        print ("No insert ...Opps...")

def create_product_dim(all_sales_df,session) -> None:
    product_dim_df = all_sales_df.with_column("Brand", split(col('MOBILE_KEY'),lit ('/'))[0])\
                                 .with_column("Model", split(col('MOBILE_KEY'),lit('/'))[1])\
                                 .with_column("Color", split(col('MOBILE_KEY'),lit ('/'))[2])\
                                 .with_column("Memory", split(col('MOBILE_KEY'),lit('/'))[3])\
                                 .select(col('mobile_key'), col('Brand'),col('Model'), col('Color'), col('Memory'))
    
    product_dim_df = product_dim_df.select(col('mobile_key'), \
                                            cast(col('Brand'),StringType()).as_("Brand"), \
                                            cast (col('Model'),StringType()).as_("Model"), \
                                            cast(col('Color'),StringType()) .as_("Color"), \
                                            cast (col('Memory'),StringType()).as_("Memory")\
                                           )
    product_dim_df = product_dim_df.groupBy(col("mobile_key"),col("Brand"),col("Model"),col("Color"),col("Memory")).count()
    product_dim_df = product_dim_df.with_column("isActive", lit('Y'))

    existing_product_dim_df = session.sql("select mobile_key, Brand, Model, Color, Memory from sales_dwh.consumption.product_dim" )
    existing_product_dim_df.count()
    product_dim_df = product_dim_df.join(existing_product_dim_df,["mobile_key", "Brand", "Model", "Color", "Memory"], join_type='leftanti')
    product_dim_df.show(5)
    product_dim_df = product_dim_df.selectExpr("sales_dwh.consumption.product_dim_seq.nextval as product_id_pk","mobile_key","Brand","Model","Color","Memory","isActive")

    product_dim_df.show(5)
    insert_cnt = int(product_dim_df.count()) 
    if insert_cnt>0:
        product_dim_df.write.save_as_table("sales_dwh.consumption.product_dim", mode="append")
        print ("save operation ran...")
    else:
        print("No insert ....occured")

def create_promocode_dim(all_sales_df, session) -> None:
    promo_code_dim_df = all_sales_df.with_column("promotion_code", expr("case when promotion_code is null then 'NA' else promotion_code end"))
    promo_code_dim_df = promo_code_dim_df.groupBy(col("promotion_code"),col("country"),col("region")).count()
    promo_code_dim_df = promo_code_dim_df.with_column("isActive",lit('Y'))
    existing_promo_code_dim_df = session.sql("select promotion_code, country, region from sales_dwh.consumption.promo_code_dim")
    promo_code_dim_df = promo_code_dim_df.join(existing_promo_code_dim_df,["promotion_code", "country", "region"],join_type='leftanti')
    promo_code_dim_df = promo_code_dim_df.selectExpr("sales_dwh.consumption.promo_code_dim_seq.nextval as promo_code_id_pk","promotion_code", "country" ,"region","isActive")

    insert_cnt = int(promo_code_dim_df.count())
    if insert_cnt>0:
        promo_code_dim_df.write.save_as_table("sales_dwh.consumption.promo_code_dim", mode="append")
        print("save operation ran...")
    else:
        print("No insert ....occured")

def create_customer_dim(all_sales_df,session) -> None:
    customer_dim_df = all_sales_df.groupBy(col("COUNTRY"), col("REGION"), col("CUSTOMER_NAME"), col("CONTACT_NO"), col("SHIPPING_ADDRESS")).count()
    customer_dim_df = customer_dim_df.with_column("isActive", lit('Y'))
    customer_dim_df = customer_dim_df.selectExpr("customer_name", "contact_no", "shipping_address", "country", "region", "isActive")

    customer_dim_df.show(5)

    existing_customer_dim_df = session.sql("select customer_name, contact_no, shipping_address, country, region from sales_dwh.consumption.customer_dim")
    customer_dim_df = customer_dim_df.join(existing_customer_dim_df,["customer_name", "contact_no", "shipping_address", "country", "region"], join_type='leftanti')
    customer_dim_df = customer_dim_df.selectExpr("sales_dwh.consumption.customer_dim_seq.nextval as customer_id_pk", "customer_name", "contact_no", "shipping_address", "country", "region","isActive")

    customer_dim_df.show(5)

    insert_cnt = int(customer_dim_df.count())

    if insert_cnt>0:
        customer_dim_df.write.save_as_table("sales_dwh.consumption.customer_dim", mode="append")
        print("save operation ran...")
    else:
        print("No insert occured ....")

    

def create_payment_dim(all_sales_df,session) -> None:
    payment_dim_df = all_sales_df.groupBy(col("COUNTRY"), col("REGION"), col("payment_method"), col("payment_provider")).count()
    payment_dim_df = payment_dim_df.with_column("isActive", lit('Y'))


    payment_dim_df.show(5)
    existing_payment_dim_df = session.sql("select payment_method, payment_provider, country, region from sales_dwh.consumption.payment_dim")

    payment_dim_df = payment_dim_df.join(existing_payment_dim_df,["payment_method", "payment_provider", "country", "region"], join_type='leftanti')

    payment_dim_df = payment_dim_df.selectExpr("sales_dwh.consumption.payment_dim_seq.nextval as payment_id_pk", "payment_method", "payment_provider", "country","region","isActive")

    payment_dim_df.show(5)

    insert_cnt = int(payment_dim_df.count())
    if insert_cnt > 0:
        payment_dim_df.write.save_as_table("sales_dwh.consumption.payment_dim", mode="append")
        print("Insertion Occured")
    else:
        print("Insertion not Occcured")
    

def create_date_dim(all_sales_df,session) -> None:
    start_date = all_sales_df.select(min(col("order_dt")).alias("min_order_dt")).collect()[0].as_dict()["MIN_ORDER_DT"]
    end_date = all_sales_df.select(max(col("order_dt")).alias("max_order_dt")).collect()[0].as_dict()["MAX_ORDER_DT"]
    print(f'start_date{start_date}')
    print(f'start_date{end_date}')
    date_range = pd.date_range(start=start_date, end=end_date,freq='D')
    date_dim = pd.DataFrame(date_range,columns=['dt'])
    date_dim['year'] = date_dim['dt'].dt.year 
    date_dim['month'] = date_dim['dt'].dt.month
    date_dim['quater'] = date_dim['dt'].dt.quarter
    date_dim['dayofweek'] = date_dim['dt'].dt.day_of_week
    date_dim['day'] = date_dim['dt'].dt.day
    date_dim['dayname'] = date_dim['dt'].dt.day_name()
    date_dim['dt'] = date_dim["dt"].dt.date

    temp_table ="snow_temp_df"
    session.write_pandas(date_dim,temp_table,auto_create_table = True,overwrite=True)

    insert_query = """
                INSERT INTO sales_dwh.consumption.date_dim(date_id_pk,order_dt,order_year,order_month,order_quater,order_day,order_dayofweek,order_dayname) 
                SELECT SALES_DWH.CONSUMPTION.DATE_DIM_SEQ.NEXTVAL,"dt","year","month","quater","day","dayofweek","dayname"
                FROM SALES_DWH.CONSUMPTION."snow_temp_df"
                """
    session.sql(insert_query).collect()
    print("Insertion Occured")
    

def main():
    session = get_snowpark_session()

    in_sales_df = session.sql("select * from sales_dwh.curated.in_sales_order")
    us_sales_df = session.sql("select * from sales_dwh.curated.us_sales_order")
    fr_sales_df = session.sql("select * from sales_dwh.curated.fr_sales_order")
    all_sales_df = in_sales_df.union(us_sales_df).union(fr_sales_df)

    create_region_dim(all_sales_df, session) 
    create_product_dim(all_sales_df,session)
    create_promocode_dim(all_sales_df, session) 
    create_customer_dim(all_sales_df,session) 
    create_payment_dim(all_sales_df,session) 
    create_date_dim(all_sales_df,session)


    date_dim_df = session.sql("select date_id_pk, order_dt from sales_dwh.consumption.date_dim")
    customer_dim_df = session.sql("select customer_id_pk,customer_name,country,region from sales_dwh.consumption.CUSTOMER_DIM")
    payment_dim_df = session.sql("select payment_id_pk, payment_method, payment_provider, country, region from sales_dwh.consumption.PAYMENT_DIM")
    product_dim_df = session.sql("select product_id_pk, mobile_key from sales_dwh.consumption.PRODUCT_DIM")
    promo_code_dim_df = session.sql("select promo_code_id_pk, promotion_code, country, region from sales_dwh.consumption.PROMO_CODE_DIM")
    region_dim_df = session.sql("select region_id_pk, country, region from sales_dwh.consumption.REGION_DIM")


    all_sales_df = all_sales_df.with_column( "promotion_code", expr("case when promotion_code is null then 'NA' else promotion_code end") )
    all_sales_df = all_sales_df.join(date_dim_df, ["order_dt"], join_type='inner')
    all_sales_df = all_sales_df.join(customer_dim_df, ["customer_name", "region", "country"], join_type='inner')
    all_sales_df = all_sales_df.join(payment_dim_df, ["payment_method", "payment_provider", "country", "region"], join_type='inner')
    
    all_sales_df = all_sales_df.join(product_dim_df, ["mobile_key"], join_type='inner')
    all_sales_df = all_sales_df.join(promo_code_dim_df, ["promotion_code", "country", "region"], join_type='inner')
    all_sales_df = all_sales_df.join(region_dim_df, ["country", "region"], join_type=' inner')

    all_sales_df.show(5)

    new_table_name = "SALES_DWH.CURATED.FACT_TB"

    all_sales_df.write.mode("append").save_as_table(new_table_name)

    insert_query = """
                INSERT INTO sales_dwh.consumption.sales_fact(order_id_pk,order_code,date_id_fk,region_id_fk,customer_id_fk,payment_id_fk,product_id_fk,promo_code_id_fk,order_quantity,local_total_order_amt,local_tax_amt,exchange_rate,eu_total_order_amt,eu_tax_amt) 
                SELECT sales_dwh.consumption.sales_fact_seq.nextval,order_id,date_id_pk,region_id_pk,customer_id_pk,payment_id_pk,product_id_pk,promo_code_id_pk,order_quantity,local_total_order_amt,local_tax_amt,exchange_rate,eu_total_order_amt,eu_tax_amt
                FROM SALES_DWH.CURATED.FACT_TB
                """
    session.sql(insert_query).collect()

    print("Succesfully Done")
    


if __name__ == '__main__':
    main()
                                          

