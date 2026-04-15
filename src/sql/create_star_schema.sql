SELECT * FROM transactions_clean tc LIMIT 10;
SELECT COUNT(*) FROM transactions_clean tc;


-- Creates Star Schema from transactions_clean table.

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
-- DIMENSION: dim_date
-- ============================================================
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date AS
SELECT DISTINCT
    CAST("InvoiceDate" AS DATE)           AS date_id,
    EXTRACT(YEAR FROM "InvoiceDate")      AS year,
    EXTRACT(QUARTER FROM "InvoiceDate")   AS quarter,
    EXTRACT(MONTH FROM "InvoiceDate")     AS month,
    EXTRACT(DAY FROM "InvoiceDate")       AS day,
    EXTRACT(DOW FROM "InvoiceDate")       AS day_of_week,
    TO_CHAR("InvoiceDate", 'Day')         AS day_name,
    TO_CHAR("InvoiceDate", 'Month')       AS month_name
FROM transactions_clean;

ALTER TABLE dim_date ADD PRIMARY KEY (date_id);

-- ============================================================
-- FACT: fact_sales
-- ============================================================
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
