use schema common;
-- create file formats csv (India), json (France), Parquet (USA) 
create or replace file format my_csv_format
type = csv
field_delimiter = ','
skip_header = 1
null_if = ('null', 'null')
empty_field_as_null = true
field_optionally_enclosed_by = '\042'
compression = auto;
-- json file format with strip outer array true 
create or replace file format my_json_format
type = json
strip_outer_array = true
compression = auto;
-- parquet file format
create or replace 
file format my_parquet_format
type = parquet
compression = snappy;

show file formats;

use schema source;
list @my_internal_stg/source=FR/;

select
t.$1:: text as order_id,
t.$2:: text as customer_name,
t.$3:: text as mobile_key,
t.$4::number as order_quantity,
t.$5::number as unit_price,
t.$6:: number as order_value,
t.$7:: text as promotion_code ,
t.$8:: number(10,2) as final_order_amount,
t.$9:: number(10,2) as tax_amount,
t.$10:: date as order_dt,
t.$11:: text as payment_status,
t.$12:: text as shipping_status,
t.$13::text as payment_method,
t.$14::text as payment_provider,
t.$15::text as mobile,
t.$16::text as shipping_address
from
@my_internal_stg/source=IN/format=csv/
(file_format => 'sales_dwh.common.my_csv_format') t;

list @my_exg_stg;

create table my_us_table 
as 
select
$1: "Order ID":: text as order_id,
$1:"Customer Name":: text as customer_name,
$1: "Mobile Model":: text as mobile_key, 
to_number($1: "Quantity") as quantity, 
to_number($1:"Price per Unit") as unit_price, 
to_decimal($1: "Total Price") as total_price, 
$1:"Promotion Code":: text as promotion_code,
$1:"Order Amount":: number (10,2) as order_amount, 
to_decimal ($1:"Tax") as tax,
$1:"Order Date":: date as order_dt,
$1:"Payment Status":: text as payment_status,
$1:"Shipping Status":: text as shipping_status,
$1:"Payment Method":: text as payment_method,
$1:"Payment Provider":: text as payment_provider,
$1:"Phone":: text as phone,
$1:"Delivery Address":: text as shipping_address
from
@my_internal_stg/source=US/format=parquet/
(file_format => 'sales_dwh.common.my_parquet_format');

select
$1:"Order ID":: text as order_id,
$1: "Customer Name":: text as customer_name,
$1: "Mobile Model":: text as mobile_key, 
to_number ($1:"Quantity") as quantity, 
to_number ($1:"Price per Unit") as unit_price, 
to_decimal ($1:"Total Price") as total_price, 
$1: "Promotion Code":: text as promotion_code,
$1: "Order Amount":: number (10,2) as order_amount, 
to_decimal ($1:"Tax") as tax,
$1: "Order Date":: date as order_dt,
$1:"Payment Status":: text as payment_status,
$1: "Shipping Status":: text as shipping_status,
$1: "Payment Method":: text as payment_method,
$1: "Payment Provider":: text as payment_provider,
$1: "Phone":: text as phone,
$1: "Delivery Address":: text as shipping_address
from @my_internal_stg/source=FR/format=json/
(file_format => 'sales_dwh.common.my_json_format');

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE;


insert into eur_ind_rate 
select 
in_exg_seq.nextval,
$1:: date as order_date,
$2:: number(10,3) as price
from 
@my_exg_stg/eur_inr.csv;
