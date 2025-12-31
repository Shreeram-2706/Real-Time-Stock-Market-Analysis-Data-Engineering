CREATE DATABASE stocks_mds;

CREATE SCHEMA stocks_mds.common;

USE stocks_mds.common;

CREATE TABLE bronze_stock_quotes_raw(
v VARIANT
);

select * from bronze_stock_quotes_raw limit 10;


