-- Creates KPI views on top of the Star Schema.
-- Run against PostgreSQL after create_star_schema.sql

-- ============================================================
-- VIEW: daily_sales
-- Daily revenue, order count, items sold
-- ============================================================
DROP VIEW IF EXISTS daily_sales;

CREATE VIEW daily_sales AS
SELECT
    d.date_id,
    d.year,
    d.month,
    d.day_name,
    COUNT(DISTINCT f.invoice_no)     AS total_orders,
    SUM(f.quantity)                  AS total_items,
    ROUND(SUM(f.amount)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(f.amount)::NUMERIC, 2) AS avg_order_value
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.date_id
ORDER BY d.date_id;

SELECT * FROM daily_sales LIMIT 10;

-- ============================================================
-- VIEW: customer_kpis
-- Per-customer: total spend, orders, items, avg order value
-- ============================================================
DROP VIEW IF EXISTS customer_kpis;

CREATE VIEW customer_kpis AS
SELECT
    c.customer_id,
    c.country,
    COUNT(DISTINCT f.invoice_no)     AS total_orders,
    SUM(f.quantity)                  AS total_items,
    ROUND(SUM(f.amount)::NUMERIC, 2) AS total_spent,
    ROUND(AVG(f.amount)::NUMERIC, 2) AS avg_order_value,
    MIN(f.date_id)                   AS first_purchase,
    MAX(f.date_id)                   AS last_purchase
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.country
ORDER BY total_spent DESC;

SELECT * FROM customer_kpis LIMIT 10;

-- ============================================================
-- VIEW: product_kpis
-- Per-product: total sold, revenue, orders, avg unit price
-- ============================================================
DROP VIEW IF EXISTS product_kpis;

CREATE VIEW product_kpis AS
SELECT
    p.stock_code,
    p.description,
    COUNT(DISTINCT f.invoice_no)    	AS total_orders,
    SUM(f.quantity)                 	AS total_sold,
    ROUND(SUM(f.amount)::NUMERIC, 2) 	AS total_revenue,
    ROUND(AVG(f.unit_price)::NUMERIC, 2) AS avg_unit_price,
    MIN(f.date_id)						AS first_sold,
    MAX(f.date_id)						AS last_sold
FROM fact_sales f
JOIN dim_product p ON f.stock_code = p.stock_code
GROUP BY p.stock_code, p.description
ORDER BY total_revenue DESC;

SELECT * FROM product_kpis LIMIT 10;

