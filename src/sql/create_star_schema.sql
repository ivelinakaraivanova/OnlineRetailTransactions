SELECT * FROM transactions_clean tc LIMIT 10;
SELECT COUNT(*) FROM transactions_clean tc;


-- Creates Star Schema from transactions_clean table
-- Run against PostgreSQL after silver_to_postgresql.py

-- ============================================================
-- DIMENSION: dim_customer
-- ============================================================
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer AS
SELECT
    DISTINCT "CustomerID" AS customer_id,
    MAX("Country") 		  AS country
FROM transactions_clean
WHERE "CustomerID" IS NOT NULL
GROUP BY "CustomerID";

ALTER TABLE dim_customer ADD PRIMARY KEY (customer_id);

-- ============================================================
-- DIMENSION: dim_product
-- ============================================================
DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product AS
SELECT DISTINCT
    "StockCode" 		AS stock_code,
    MAX("Description") 	AS description
FROM transactions_clean
GROUP BY "StockCode";

ALTER TABLE dim_product ADD PRIMARY KEY (stock_code);

-- ============================================================
-- DIMENSION: dim_date (continuous — no gaps)
-- ============================================================
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date AS
WITH date_range AS (
    SELECT generate_series(
        (SELECT MIN(CAST("InvoiceDate" AS DATE)) FROM transactions_clean),
        (SELECT MAX(CAST("InvoiceDate" AS DATE)) FROM transactions_clean),
        '1 day'::interval
    )::date AS date_id
)
SELECT
    date_id,
    EXTRACT(YEAR FROM date_id)      AS year,
    EXTRACT(QUARTER FROM date_id)   AS quarter,
    EXTRACT(MONTH FROM date_id)     AS month,
    EXTRACT(DAY FROM date_id)       AS day,
    EXTRACT(DOW FROM date_id)       AS day_of_week,
    TO_CHAR(date_id, 'Day')         AS day_name,
    TO_CHAR(date_id, 'Month')       AS month_name
FROM date_range;

ALTER TABLE dim_date ADD PRIMARY KEY (date_id);

-- ============================================================
-- FACT: fact_sales
-- ============================================================
-- DROP TABLE fact_sales CASCADE;

CREATE TABLE fact_sales AS
SELECT
    "InvoiceNo"                           AS invoice_no,
    "StockCode"                           AS stock_code,
    CAST("InvoiceDate" AS DATE)           AS date_id,
    "CustomerID"                          AS customer_id,
    "Quantity"							  AS quantity,
    "UnitPrice"                           AS unit_price,
    "Amount"							  AS amount
FROM transactions_clean;

ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_product FOREIGN KEY (stock_code) REFERENCES dim_product(stock_code),
    ADD CONSTRAINT fk_fact_date    FOREIGN KEY (date_id)    REFERENCES dim_date(date_id);

-- No FK on customer_id because some rows have NULL CustomerID

SELECT * FROM dim_date dd LIMIT 10;
