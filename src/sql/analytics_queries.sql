
-- ============================================================
-- Top 10 revenue days
-- ============================================================
SELECT date_id, total_orders, total_revenue
FROM daily_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================================================
-- Monthly revenue trend
-- ============================================================
SELECT year, month,
       SUM(total_revenue) AS monthly_revenue,
       SUM(total_orders)  AS monthly_orders
FROM daily_sales
GROUP BY year, month
ORDER BY year, month;

-- ============================================================
-- Top 10 customers by total spend
-- ============================================================
SELECT customer_id, country, total_orders, total_spent
FROM customer_kpis
ORDER BY total_spent DESC
LIMIT 10;

-- ============================================================
-- Top 10 products by revenue
-- ============================================================
SELECT stock_code, description, total_sold, total_revenue
FROM product_kpis
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================================================
-- Revenue by country
-- ============================================================
SELECT c.country,
       COUNT(DISTINCT f.invoice_no)     AS total_orders,
       ROUND(SUM(f.amount)::NUMERIC, 2) AS total_revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_revenue DESC;

-- ============================================================
-- Day-of-week sales pattern
-- ============================================================
SELECT d.day_name,
       ROUND(AVG(ds.total_revenue)::NUMERIC, 2) AS avg_daily_revenue,
       ROUND(AVG(ds.total_orders)::NUMERIC, 0)  AS avg_daily_orders
FROM daily_sales ds
JOIN dim_date d ON ds.date_id = d.date_id
GROUP BY d.day_name, d.day_of_week
ORDER BY d.day_of_week;

-- ============================================================
-- Customer retention — one-time vs repeat buyers
-- ============================================================
SELECT
    CASE WHEN total_orders = 1 THEN 'One-time' ELSE 'Repeat' END AS buyer_type,
    COUNT(*)                        AS customer_count,
    ROUND(AVG(total_spent)::NUMERIC, 2) AS avg_spend
FROM customer_kpis
GROUP BY buyer_type;

-- ============================================================
-- Products with no sales in last 3 months of data
-- ============================================================
SELECT p.stock_code, p.description, pk.last_sold, pk.total_revenue
FROM product_kpis pk
JOIN dim_product p ON pk.stock_code = p.stock_code
WHERE pk.last_sold < (SELECT MAX(date_id) - INTERVAL '90 days' FROM dim_date)
ORDER BY pk.total_revenue DESC
LIMIT 20;