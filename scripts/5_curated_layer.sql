use schema curated;


drop sequence in_sales_order_seq;
drop sequence us_sales_order_seq;
drop sequence fr_sales_order_seq;

create or replace sequence in_sales_order_seq
start = 1
increment = 1
comment= 'This is sequence for India sales order table';
create or replace sequence us_sales_order_seq
start = 1
increment = 1
comment= 'This is sequence for USA sales order table';
create or replace sequence fr_sales_order_seq
start = 1
increment = 1
comment= 'This is sequence for France sales order table';

select * from us_sales_order;
create or replace table in_sales_order( 
sales_order_key number(38,0),
order_id varchar(),
order_dt date,
customer_name varchar(), 
mobile_key varchar(),
country varchar(),
region varchar(),
order_quantity number(38,0),
local_currency varchar(),
local_unit_price number(38,0), 
promotion_code varchar(),
local_total_order_amt number(10,2),
local_tax_amt number(10,2),
exchange_rate number(15,3),
eu_total_order_amt number(23,3),
eu_tax_amt number(23,3),
payment_status varchar(),
shipping_status varchar(),
payment_method varchar(),
payment_provider varchar(),
contact_no varchar(),
shipping_address varchar()
);

create or replace table us_sales_order(
sales_order_key number(38,0),
order_id varchar(),
order_dt date,
customer_name varchar(),
mobile_key varchar(),
country varchar(), 
region varchar(),
order_quantity number(38,0),
local_currency varchar(),
local_unit_price number(38,0),
promotion_code varchar(), 
local_total_order_amt number(10,2),
local_tax_amt number(10,2),
exchange_rate number(15,3),
eu_total_order_amt number(23,3),
eu_tax_amt number(23,3),
payment_status varchar(),
shipping_status varchar(),
payment_method varchar(), 
payment_provider varchar (),
contact_no varchar(),
shipping_address varchar()
);

create or replace table fr_sales_order(
sales_order_key number(38,0),
order_id varchar(),
order_dt date,
customer_name varchar(),
mobile_key varchar(),
country varchar(), 
region varchar(),
order_quantity number(38,0),
local_currency varchar(),
local_unit_price number(38,0),
promotion_code varchar(), 
local_total_order_amt number(10,2),
local_tax_amt number(10,2),
exchange_rate number(15,3),
eu_total_order_amt number(23,3),
eu_tax_amt number(23,3),
payment_status varchar(),
shipping_status varchar(),
payment_method varchar(), 
payment_provider varchar (),
contact_no varchar(),
shipping_address varchar()
);




