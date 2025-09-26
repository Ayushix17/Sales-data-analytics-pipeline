-- Sales Data Analytics SQL Queries
-- These queries support the Power BI dashboard KPIs

-- 1. REVENUE GROWTH ANALYSIS
-- Monthly revenue growth with year-over-year comparison
WITH monthly_revenue AS (
    SELECT 
        strftime('%Y', transaction_date) as year,
        strftime('%m', transaction_date) as month,
        strftime('%Y-%m', transaction_date) as year_month,
        SUM(total_amount) as revenue,
        COUNT(*) as transaction_count,
        COUNT(DISTINCT customer_id) as unique_customers,
        AVG(total_amount) as avg_transaction_value
    FROM sales_transactions
    GROUP BY strftime('%Y-%m', transaction_date)
),
revenue_growth AS (
    SELECT *,
        LAG(revenue, 1) OVER (ORDER BY year_month) as prev_month_revenue,
        LAG(revenue, 12) OVER (ORDER BY year_month) as prev_year_revenue,
        CASE 
            WHEN LAG(revenue, 1) OVER (ORDER BY year_month) IS NOT NULL 
            THEN (revenue - LAG(revenue, 1) OVER (ORDER BY year_month)) / LAG(revenue, 1) OVER (ORDER BY year_month) * 100
            ELSE 0 
        END as mom_growth_rate,
        CASE 
            WHEN LAG(revenue, 12) OVER (ORDER BY year_month) IS NOT NULL 
            THEN (revenue - LAG(revenue, 12) OVER (ORDER BY year_month)) / LAG(revenue, 12) OVER (ORDER BY year_month) * 100
            ELSE 0 
        END as yoy_growth_rate
    FROM monthly_revenue
)
SELECT * FROM revenue_growth ORDER BY year_month;

-- 2. CUSTOMER CHURN ANALYSIS
-- Identify churned customers (no purchases in last 90 days)
WITH customer_last_purchase AS (
    SELECT 
        customer_id,
        MAX(transaction_date) as last_purchase_date,
        COUNT(*) as total_transactions,
        SUM(total_amount) as total_spent,
        AVG(total_amount) as avg_transaction_value,
        julianday('now') - julianday(MAX(transaction_date)) as days_since_last_purchase
    FROM sales_transactions
    GROUP BY customer_id
),
customer_status AS (
    SELECT *,
        CASE 
            WHEN days_since_last_purchase <= 30 THEN 'Active'
            WHEN days_since_last_purchase <= 90 THEN 'At Risk'
            ELSE 'Churned'
        END as customer_status
    FROM customer_last_purchase
)
SELECT 
    customer_status,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    AVG(total_spent) as avg_customer_value,
    AVG(total_transactions) as avg_transactions_per_customer
FROM customer_status
GROUP BY customer_status;

-- 3. PRODUCT PERFORMANCE ANALYSIS
-- Top performing products with trend analysis
SELECT 
    p.category,
    p.product_name,
    p.brand,
    SUM(st.quantity) as units_sold,
    SUM(st.total_amount) as total_revenue,
    COUNT(DISTINCT st.customer_id) as unique_buyers,
    COUNT(*) as transaction_count,
    AVG(st.total_amount) as avg_order_value,
    ROUND(p.profit_margin, 2) as profit_margin,
    ROUND(SUM(st.total_amount) * p.profit_margin / 100, 2) as estimated_profit
FROM sales_transactions st
JOIN products p ON st.product_id = p.product_id
GROUP BY p.product_id, p.category, p.product_name, p.brand, p.profit_margin
ORDER BY total_revenue DESC
LIMIT 50;

-- 4. CATEGORY PERFORMANCE WITH TRENDS
-- Product category analysis with monthly trends
SELECT 
    p.category,
    strftime('%Y-%m', st.transaction_date) as month,
    SUM(st.total_amount) as monthly_revenue,
    SUM(st.quantity) as units_sold,
    COUNT(DISTINCT st.customer_id) as unique_customers,
    AVG(st.total_amount) as avg_transaction_value
FROM sales_transactions st
JOIN products p ON st.product_id = p.product_id
GROUP BY p.category, strftime('%Y-%m', st.transaction_date)
ORDER BY p.category, month;

-- 5. CUSTOMER SEGMENTATION ANALYSIS
-- RFM Analysis (Recency, Frequency, Monetary)
WITH customer_rfm AS (
    SELECT 
        customer_id,
        julianday('now') - julianday(MAX(transaction_date)) as recency_days,
        COUNT(*) as frequency,
        SUM(total_amount) as monetary_value,
        AVG(total_amount) as avg_order_value
    FROM sales_transactions
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT *,
        CASE 
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END as recency_score,
        CASE 
            WHEN frequency >= 20 THEN 5
            WHEN frequency >= 15 THEN 4
            WHEN frequency >= 10 THEN 3
            WHEN frequency >= 5 THEN 2
            ELSE 1
        END as frequency_score,
        CASE 
            WHEN monetary_value >= 5000 THEN 5
            WHEN monetary_value >= 2000 THEN 4
            WHEN monetary_value >= 1000 THEN 3
            WHEN monetary_value >= 500 THEN 2
            ELSE 1
        END as monetary_score
    FROM customer_rfm
),
customer_segments AS (
    SELECT *,
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost Customers'
            ELSE 'Potential Loyalists'
        END as customer_segment
    FROM rfm_scores
)
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(monetary_value), 2) as avg_customer_value,
    ROUND(AVG(frequency), 1) as avg_purchase_frequency,
    ROUND(AVG(recency_days), 0) as avg_days_since_last_purchase
FROM customer_segments
GROUP BY customer_segment
ORDER BY customer_count DESC;

-- 6. REGIONAL PERFORMANCE
-- Sales performance by region with growth metrics
WITH regional_monthly AS (
    SELECT 
        region,
        strftime('%Y-%m', transaction_date) as month,
        SUM(total_amount) as revenue,
        COUNT(*) as transactions,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM sales_transactions
    GROUP BY region, strftime('%Y-%m', transaction_date)
),
regional_growth AS (
    SELECT *,
        LAG(revenue, 1) OVER (PARTITION BY region ORDER BY month) as prev_month_revenue,
        CASE 
            WHEN LAG(revenue, 1) OVER (PARTITION BY region ORDER BY month) IS NOT NULL 
            THEN (revenue - LAG(revenue, 1) OVER (PARTITION BY region ORDER BY month)) / 
                 LAG(revenue, 1) OVER (PARTITION BY region ORDER BY month) * 100
            ELSE 0 
        END as growth_rate
    FROM regional_monthly
)
SELECT * FROM regional_growth ORDER BY region, month;

-- 7. SALES REP PERFORMANCE
-- Individual sales representative performance metrics
SELECT 
    sales_rep,
    COUNT(*) as total_transactions,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction_value,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT product_id) as products_sold,
    ROUND(SUM(total_amount) / COUNT(DISTINCT customer_id), 2) as revenue_per_customer,
    strftime('%Y-%m', MIN(transaction_date)) as first_sale_month,
    strftime('%Y-%m', MAX(transaction_date)) as latest_sale_month
FROM sales_transactions
GROUP BY sales_rep
ORDER BY total_revenue DESC;

-- 8. SEASONAL ANALYSIS
-- Sales patterns by day of week, month, and quarter
SELECT 
    'Day of Week' as period_type,
    CASE CAST(strftime('%w', transaction_date) AS INTEGER)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as period_value,
    SUM(total_amount) as total_revenue,
    COUNT(*) as transaction_count,
    AVG(total_amount) as avg_transaction_value
FROM sales_transactions
GROUP BY strftime('%w', transaction_date)

UNION ALL

SELECT 
    'Month' as period_type,
    CASE CAST(strftime('%m', transaction_date) AS INTEGER)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END as period_value,
    SUM(total_amount) as total_revenue,
    COUNT(*) as transaction_count,
    AVG(total_amount) as avg_transaction_value
FROM sales_transactions
GROUP BY strftime('%m', transaction_date)

UNION ALL

SELECT 
    'Quarter' as period_type,
    'Q' || CASE 
        WHEN CAST(strftime('%m', transaction_date) AS INTEGER) IN (1,2,3) THEN '1'
        WHEN CAST(strftime('%m', transaction_date) AS INTEGER) IN (4,5,6) THEN '2'
        WHEN CAST(strftime('%m', transaction_date) AS INTEGER) IN (7,8,9) THEN '3'
        ELSE '4'
    END as period_value,
    SUM(total_amount) as total_revenue,
    COUNT(*) as transaction_count,
    AVG(total_amount) as avg_transaction_value
FROM sales_transactions
GROUP BY CASE 
    WHEN CAST(strftime('%m', transaction_date) AS INTEGER) IN (1,2,3) THEN '1'
    WHEN CAST(strftime('%m', transaction_date) AS INTEGER) IN (4,5,6) THEN '2'
    WHEN CAST(strftime('%m', transaction_date) AS INTEGER) IN (7,8,9) THEN '3'
    ELSE '4'
END
ORDER BY period_type, period_value;

-- 9. INVENTORY INSIGHTS
-- Product inventory status and reorder recommendations
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.stock_quantity,
    COALESCE(recent_sales.units_sold_30_days, 0) as units_sold_last_30_days,
    COALESCE(recent_sales.avg_daily_sales, 0) as avg_daily_sales,
    CASE 
        WHEN COALESCE(recent_sales.avg_daily_sales, 0) > 0 
        THEN ROUND(p.stock_quantity / recent_sales.avg_daily_sales, 0)
        ELSE 999
    END as days_of_inventory,
    CASE 
        WHEN p.stock_quantity <= 10 THEN 'Critical'
        WHEN p.stock_quantity <= 50 THEN 'Low'
        WHEN p.stock_quantity <= 100 THEN 'Medium'
        ELSE 'High'
    END as stock_status,
    CASE 
        WHEN COALESCE(recent_sales.avg_daily_sales, 0) > 0 AND 
             p.stock_quantity / recent_sales.avg_daily_sales < 14 
        THEN 'Reorder Required'
        ELSE 'OK'
    END as reorder_recommendation
FROM products p
LEFT JOIN (
    SELECT 
        product_id,
        SUM(quantity) as units_sold_30_days,
        AVG(quantity * 1.0) as avg_daily_sales
    FROM sales_transactions
    WHERE transaction_date >= date('now', '-30 days')
    GROUP BY product_id
) recent_sales ON p.product_id = recent_sales.product_id
WHERE p.is_active = 1
ORDER BY days_of_inventory ASC;

-- 10. EXECUTIVE SUMMARY KPIs
-- High-level metrics for executive dashboard
SELECT 
    'Total Revenue' as metric,
    '$' || printf('%.2f', SUM(total_amount)) as current_value,
    '$' || printf('%.2f', (SELECT SUM(total_amount) FROM sales_transactions 
                          WHERE transaction_date >= date('now', '-30 days'))) as last_30_days,
    NULL as growth_rate
FROM sales_transactions

UNION ALL

SELECT 
    'Total Customers' as metric,
    CAST(COUNT(DISTINCT customer_id) AS TEXT) as current_value,
    CAST((SELECT COUNT(DISTINCT customer_id) FROM sales_transactions 
          WHERE transaction_date >= date('now', '-30 days')) AS TEXT) as last_30_days,
    NULL as growth_rate
FROM sales_transactions

UNION ALL

SELECT 
    'Average Order Value' as metric,
    '$' || printf('%.2f', AVG(total_amount)) as current_value,
    '$' || printf('%.2f', (SELECT AVG(total_amount) FROM sales_transactions 
                          WHERE transaction_date >= date('now', '-30 days'))) as last_30_days,
    NULL as growth_rate
FROM sales_transactions

UNION ALL

SELECT 
    'Active Products' as metric,
    CAST((SELECT COUNT(*) FROM products WHERE is_active = 1) AS TEXT) as current_value,
    CAST((SELECT COUNT(DISTINCT product_id) FROM sales_transactions 
          WHERE transaction_date >= date('now', '-30 days')) AS TEXT) as last_30_days,
    NULL as growth_rate;