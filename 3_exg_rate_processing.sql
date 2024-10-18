use schema source;

list @my_exg_stg;

select * from final_exg_tbl

create or replace sequence in_exg_seq
start = 1
increment = 1
comment= 'This is sequence for India exg rate';

create or replace sequence us_exg_seq
start = 1
increment = 1
comment= 'This is sequence for USA exg rate';

create or replace sequence exg_seq
start = 1
increment = 1
comment= 'This is sequence for exg rate';


create or replace table eur_usd_rate(
us_exg_key number(38,0),
exchange_rate_dt date,
eu2usd number(10,3)
);

create or replace table eur_ind_rate(
in_exg_key number(38,0),
exchange_rate_dt date,
eu2inr number(10,3)
);

create or replace table exg_rate(
exg_key number(38,0),
exchange_rate_dt date,
eu int,
eu2usd number(10,3),
eu2inr number(10,3)
);
