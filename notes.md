docker-compose -f docker-compose-LocalExecutor.yml down
docker-compose -f docker-compose-LocalExecutor.yml up -d

pgcli -h localhost -p 5432 -U airflow -d airflow
pass also airflow 

setup/postgres/create_user_purchase.sql - to reload the data in postgres below 

CREATE SCHEMA retail;

CREATE TABLE retail.user_purchase (
    invoice_number varchar(10),
    stock_code varchar(20),
    detail varchar(1000),
    quantity int,
    invoice_date timestamp,
    unit_price Numeric(8,3),
    customer_id int,
    country varchar(20)
);

COPY retail.user_purchase(invoice_number,
stock_code,detail,quantity,
invoice_date,unit_price,customer_id,country) 
FROM '/data/retail/OnlineRetail.csv' 
DELIMITER ','  CSV HEADER;


# EMR config 


# Redshift 

pgcli -h <your-redshift-cluster> -U <your-user> -p 5439 -d <your-database>
