create or replace file format unload_csv_format
type = csv
compression = 'NONE'
field_delimiter = ','
record_delimiter = '\n'
file_extension = 'csv'
field_optionally_enclosed_by = '\042';

create or replace stage consumption.my_stg;

list @consumption.my_stg;

COPY INTO @my_stg/sales_data/date_dim
FROM sales_dwh.consumption.date_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/sales_data/customer_dim
FROM SALES_DWH.CONSUMPTION.CUSTOMER_DIM
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/sales_data/payment_dim
FROM SALES_DWH.CONSUMPTION.payment_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/sales_data/product_dim
FROM SALES_DWH.CONSUMPTION.PRODUCT_DIM
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/sales_data/promo_dim
FROM SALES_DWH.CONSUMPTION.PROMO_CODE_DIM
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/sales_data/region_dim
FROM SALES_DWH.CONSUMPTION.REGION_DIM
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/sales_data/sales_fact
FROM SALES_DWH.CONSUMPTION.SALES_FACT
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;









