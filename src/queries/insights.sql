-- Athena SQL queries for ecommerce data lake insights
-- Run these queries in AWS Athena console after setting up Glue catalog

-- =========================================================================
-- SETUP: CREATE EXTERNAL TABLES FOR CURATED DATA (if not using Glue crawler)
-- =========================================================================

-- If you prefer manual table creation instead of Glue crawlers, uncomment and run:
/*
-- Create external table for comprehensive curated view
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.comprehensive_view (
    county STRING,
    category STRING,
    product_id STRING,
    product_name STRING,
    total_revenue DOUBLE,
    total_quantity BIGINT,
    transaction_count BIGINT,
    avg_price DOUBLE,
    max_price DOUBLE,
    min_price DOUBLE
)
PARTITIONED BY (county STRING)
STORED AS PARQUET
LOCATION 's3://[your-bucket]/curated/comprehensive_view/';

-- Create external table for sales by county
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.sales_by_county (
    county STRING,
    total_revenue DOUBLE,
    total_quantity BIGINT,
    transaction_count BIGINT,
    unique_products BIGINT,
    avg_transaction_value DOUBLE
)
PARTITIONED BY (county STRING)
STORED AS DELTA
LOCATION 's3://[your-bucket]/curated/sales_by_county/';

-- Create external table for top items per county
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.top_items_per_county (
    county STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    total_revenue DOUBLE,
    total_quantity BIGINT,
    transaction_count BIGINT,
    revenue_rank BIGINT
)
PARTITIONED BY (county STRING)
STORED AS DELTA
LOCATION 's3://[your-bucket]/curated/top_items_per_county/';

-- Create external table for sales by product
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.sales_by_product (
    product_id STRING,
    product_name STRING,
    category STRING,
    total_revenue DOUBLE,
    total_quantity BIGINT,
    avg_price DOUBLE,
    transaction_count BIGINT,
    max_price DOUBLE,
    min_price DOUBLE
)
PARTITIONED BY (category STRING)
STORED AS DELTA
LOCATION 's3://[your-bucket]/curated/sales_by_product/';

-- Create external table for sales by category
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.sales_by_category (
    category STRING,
    total_revenue DOUBLE,
    total_quantity BIGINT,
    transaction_count BIGINT,
    unique_products BIGINT,
    avg_price DOUBLE
)
STORED AS DELTA
LOCATION 's3://[your-bucket]/curated/sales_by_category/';

-- Repair partitions to make them visible to Athena
MSCK REPAIR TABLE ecommerce_db.comprehensive_view;
MSCK REPAIR TABLE ecommerce_db.sales_by_county;
MSCK REPAIR TABLE ecommerce_db.top_items_per_county;
MSCK REPAIR TABLE ecommerce_db.sales_by_product;
MSCK REPAIR TABLE ecommerce_db.sales_by_category;
*/

-- =========================================================================
-- BUSINESS INSIGHTS QUERIES
-- =========================================================================

-- 1. Top 10 counties by total revenue
SELECT 
    county,
    SUM(total_revenue) as total_revenue,
    SUM(total_quantity) as total_quantity,
    COUNT(*) as product_count
FROM ecommerce_db.comprehensive_view
GROUP BY county
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Top 10 product categories by revenue across all counties
SELECT 
    category,
    SUM(total_revenue) as total_revenue,
    SUM(total_quantity) as total_quantity,
    COUNT(DISTINCT county) as counties_served,
    AVG(avg_price) as avg_category_price
FROM ecommerce_db.sales_by_category
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Top 10 selling products by revenue in Kenya
SELECT 
    product_id,
    product_name,
    category,
    SUM(total_revenue) as total_revenue,
    SUM(total_quantity) as total_quantity,
    SUM(transaction_count) as total_transactions
FROM ecommerce_db.sales_by_product
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC
LIMIT 10;

-- 4. Top 10 selling items per Kenyan county (as requested)
SELECT 
    county,
    product_category,
    SUM(total_revenue) as total_revenue,
    SUM(total_quantity) as total_quantity,
    COUNT(DISTINCT product_id) as unique_products
FROM ecommerce_db.comprehensive_view
GROUP BY county, product_category
ORDER BY total_revenue DESC
LIMIT 10;

-- 5. Revenue distribution by county and category
SELECT 
    county,
    category,
    SUM(total_revenue) as total_revenue,
    SUM(total_quantity) as total_quantity,
    ROUND(SUM(total_revenue) * 100.0 / SUM(SUM(total_revenue)) OVER (PARTITION BY county), 2) as revenue_percentage_by_county
FROM ecommerce_db.comprehensive_view
GROUP BY county, category
ORDER BY county, total_revenue DESC;

-- 6. Average transaction value by county
SELECT 
    county,
    AVG(avg_transaction_value) as avg_transaction_value,
    SUM(total_revenue) as total_revenue,
    COUNT(*) as transaction_count
FROM ecommerce_db.sales_by_county
GROUP BY county
ORDER BY avg_transaction_value DESC;

-- 7. Top performing products per county (top 5 per county)
WITH county_product_ranking AS (
    SELECT 
        county,
        product_id,
        product_name,
        category,
        total_revenue,
        ROW_NUMBER() OVER (PARTITION BY county ORDER BY total_revenue DESC) as rank_in_county
    FROM ecommerce_db.top_items_per_county
)
SELECT 
    county,
    product_id,
    product_name,
    category,
    total_revenue,
    rank_in_county
FROM county_product_ranking
WHERE rank_in_county <= 5
ORDER BY county, rank_in_county;

-- 8. Category performance analysis
SELECT 
    category,
    COUNT(DISTINCT county) as counties_available,
    SUM(total_revenue) as total_revenue,
    SUM(total_quantity) as total_quantity,
    AVG(avg_price) as avg_price,
    COUNT(DISTINCT product_id) as unique_products
FROM ecommerce_db.comprehensive_view
GROUP BY category
ORDER BY total_revenue DESC;

-- 9. Price range analysis by category
SELECT 
    category,
    MIN(min_price) as lowest_price,
    AVG(avg_price) as avg_price,
    MAX(max_price) as highest_price,
    MAX(max_price) - MIN(min_price) as price_range
FROM ecommerce_db.sales_by_product
GROUP BY category
ORDER BY avg_price DESC;

-- 10. Market penetration analysis
SELECT 
    county,
    COUNT(DISTINCT category) as categories_served,
    COUNT(DISTINCT product_id) as unique_products_offered,
    SUM(total_revenue) as total_revenue,
    SUM(total_quantity) as total_quantity
FROM ecommerce_db.comprehensive_view
GROUP BY county
ORDER BY categories_served DESC, total_revenue DESC;

-- =========================================================================
-- TIME-BASED ANALYSIS (if date column is available)
-- =========================================================================

-- 11. Monthly revenue trends (if date partitioning is available)
/*
SELECT 
    date_trunc('month', date) as month,
    county,
    SUM(total_revenue) as monthly_revenue,
    SUM(total_quantity) as monthly_quantity
FROM ecommerce_db.comprehensive_view
WHERE date >= date_add('month', -6, current_date)
GROUP BY date_trunc('month', date), county
ORDER BY month DESC, monthly_revenue DESC;
*/

-- =========================================================================
-- PERFORMANCE OPTIMIZATION TIPS
-- =========================================================================

-- For better performance with large datasets:
-- 1. Use partition pruning: WHERE county = 'Nairobi'
-- 2. Use column pruning: SELECT only needed columns
-- 3. Consider using CTAS for complex queries
-- 4. Use result caching for frequently run queries

-- Example of optimized query for specific county:
SELECT 
    product_name,
    category,
    total_revenue,
    total_quantity
FROM ecommerce_db.comprehensive_view
WHERE county = 'Nairobi'
ORDER BY total_revenue DESC
LIMIT 100;

-- =========================================================================
-- DATA QUALITY CHECKS
-- =========================================================================

-- Check for data quality issues
SELECT 
    'Negative revenue' as issue_type,
    COUNT(*) as issue_count
FROM ecommerce_db.comprehensive_view
WHERE total_revenue < 0

UNION ALL

SELECT 
    'Zero quantity' as issue_type,
    COUNT(*) as issue_count
FROM ecommerce_db.comprehensive_view
WHERE total_quantity = 0

UNION ALL

SELECT 
    'Null product names' as issue_type,
    COUNT(*) as issue_count
FROM ecommerce_db.comprehensive_view
WHERE product_name IS NULL OR product_name = '';
