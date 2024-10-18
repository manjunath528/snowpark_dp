-- create database
create database if not exists sales_dwh;
use database sales_dwh;
create schema if not exists source; -- will have source stage etc 
create schema if not exists curated; -- data curation and de-duplication 
create schema if not exists consumption; -- fact & dimension 
create schema if not exists audit; -- to capture all audit records 
create schema if not exists common; -- for file formats sequence


use schema source;
create or replace stage my_internal_stg;

DESC STAGE my_internal_stg;

LIST @my_internal_stg;
LIST @my_internal_stg/source/source=US/;

create or replace stage my_exg_stg;

list @my_exg_stg;
