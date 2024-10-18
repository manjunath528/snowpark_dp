use schema source;
drop sequence in_sales_seq;
drop sequence us_sales_seq;
drop sequence fr_sales_seq;
select max(exchange_rate_dt) from SALES_DWH.SOURCE.EXG_RATE;


create or replace sequence in_sales_seq
start = 1
increment = 1
comment= 'This is sequence for India sales order table';

create or replace sequence us_sales_seq
start = 1
increment = 1
comment= 'This is sequence for USA sales order table';

create or replace sequence fr_sales_seq
start = 1
increment = 1
comment= 'This is sequence for France sales order table';

create or replace transient table in_sales_order (
sales_order_key number(38,0), 
order_id varchar(), 
customer_name varchar(), 
mobile_key varchar(), 
order_quantity number(38,0), 
unit_price number(38,0), 
order_valaue number(38,0),
promotion_code varchar(), 
final_order_amount number(10,2), 
tax_amount number(10,2) , 
order_dt date, 
payment_status varchar(), 
shipping_status varchar(), 
payment_method varchar(), 
payment_provider varchar(), 
mobile varchar(),
shipping_address varchar(), 
_metadata_file_name varchar(),
_metadata_row_numer number(38, 0),
_metadata_last_modified timestamp_ntz(9)
);

create or replace transient table us_sales_order (
sales_order_key number(38,0), 
order_id varchar(), 
customer_name varchar(), 
mobile_key varchar(), 
order_quantity number(10,2), 
unit_price number(10,2), 
order_value number(10,2), 
promotion_code varchar(),
final_order_amount number(10,2) , 
tax_amount number(10,2), 
order_dt date, 
payment_status varchar(), 
shipping_status varchar(), 
payment_method varchar(), 
payment_provider varchar(), 
phone varchar(), 
shipping_address varchar(),
_metadata_file_name varchar(),
_metadata_row_numer number(38,0),
_metadata_last_modified timestamp_ntz(9)
) ;


create or replace transient table fr_sales_order (
sales_order_key number(38,0), 
order_id varchar(), 
customer_name varchar(), 
mobile_key varchar(), 
order_quantity number(10,2), 
unit_price number(10,2), 
order_value number(10,2), 
promotion_code varchar(),
final_order_amount number(10,2) , 
tax_amount number(10,2), 
order_dt date, 
payment_status varchar(), 
shipping_status varchar(), 
payment_method varchar(), 
payment_provider varchar(), 
phone varchar(), 
shipping_address varchar(),
_metadata_file_name varchar(),
_metadata_row_numer number(38,0),
_metadata_last_modified timestamp_ntz(9)
) ;


show tables;