-- Топ-10 самых продаваемых продуктов
CREATE VIEW report_top10_products_by_quantity AS
SELECT
    product_name,
    product_category,
    SUM(sale_quantity)    AS total_quantity_sold,
    SUM(sale_total_price) AS total_revenue
FROM mock_data
GROUP BY product_name, product_category
ORDER BY total_quantity_sold DESC
LIMIT 10;

-- Общая выручка по категориям продуктов
CREATE VIEW report_revenue_by_category AS
SELECT
    product_category,
    COUNT(*)              AS total_sales,
    SUM(sale_quantity)    AS total_quantity_sold,
    SUM(sale_total_price) AS total_revenue
FROM mock_data
GROUP BY product_category
ORDER BY total_revenue DESC;

-- Средний рейтинг и количество отзывов для каждого продукта
CREATE VIEW report_product_ratings AS
SELECT
    product_name,
    product_category,
    product_brand,
    ROUND(AVG(product_rating), 2) AS avg_rating,
    SUM(product_reviews)          AS total_reviews,
    COUNT(*)                      AS sale_records
FROM mock_data
GROUP BY product_name, product_category, product_brand
ORDER BY avg_rating DESC;


-- Топ-10 клиентов с наибольшей суммой покупок
CREATE VIEW report_top10_customers_by_revenue AS
SELECT
    sale_customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_country,
    SUM(sale_total_price)           AS total_spent,
    SUM(sale_quantity)              AS total_items_bought,
    COUNT(*)                        AS total_orders,
    ROUND(AVG(sale_total_price), 2) AS avg_order_value
FROM mock_data
GROUP BY sale_customer_id, customer_first_name, customer_last_name,
         customer_email, customer_country
ORDER BY total_spent DESC
LIMIT 10;

-- Распределение клиентов по странам
CREATE VIEW report_customers_by_country AS
SELECT
    customer_country,
    COUNT(DISTINCT sale_customer_id) AS unique_customers,
    SUM(sale_total_price)            AS total_revenue,
    ROUND(AVG(sale_total_price), 2)  AS avg_order_value
FROM mock_data
GROUP BY customer_country
ORDER BY unique_customers DESC;

-- Средний чек для каждого клиента
CREATE VIEW report_avg_order_per_customer AS
SELECT
    sale_customer_id,
    customer_first_name,
    customer_last_name,
    customer_country,
    COUNT(*)                        AS total_orders,
    SUM(sale_total_price)           AS total_spent,
    ROUND(AVG(sale_total_price), 2) AS avg_order_value
FROM mock_data
GROUP BY sale_customer_id, customer_first_name, customer_last_name, customer_country
ORDER BY avg_order_value DESC;


-- Месячные тренды продаж
CREATE VIEW report_sales_trends_monthly AS
SELECT
    EXTRACT(YEAR  FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS sale_year,
    EXTRACT(MONTH FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS sale_month,
    COUNT(*)              AS total_orders,
    SUM(sale_quantity)    AS total_quantity,
    SUM(sale_total_price) AS total_revenue
FROM mock_data
WHERE sale_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
GROUP BY sale_year, sale_month
ORDER BY sale_year, sale_month;

-- Годовые тренды продаж
CREATE VIEW report_sales_trends_yearly AS
SELECT
    EXTRACT(YEAR FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS sale_year,
    COUNT(*)              AS total_orders,
    SUM(sale_quantity)    AS total_quantity,
    SUM(sale_total_price) AS total_revenue
FROM mock_data
WHERE sale_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
GROUP BY sale_year
ORDER BY sale_year;

-- Средний размер заказа по месяцам
CREATE VIEW report_avg_order_by_month AS
SELECT
    EXTRACT(YEAR  FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS sale_year,
    EXTRACT(MONTH FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS sale_month,
    ROUND(AVG(sale_total_price), 2) AS avg_order_value,
    ROUND(AVG(sale_quantity), 2)    AS avg_quantity_per_order
FROM mock_data
WHERE sale_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
GROUP BY sale_year, sale_month
ORDER BY sale_year, sale_month;


-- Топ-5 магазинов по выручке
CREATE VIEW report_top5_stores_by_revenue AS
SELECT
    store_name,
    store_city,
    store_country,
    SUM(sale_total_price)           AS total_revenue,
    COUNT(*)                        AS total_orders,
    ROUND(AVG(sale_total_price), 2) AS avg_order_value
FROM mock_data
GROUP BY store_name, store_city, store_country
ORDER BY total_revenue DESC
LIMIT 5;

-- Распределение продаж по городам и странам
CREATE VIEW report_sales_by_store_location AS
SELECT
    store_country,
    store_city,
    store_state,
    COUNT(DISTINCT store_name)      AS store_count,
    SUM(sale_total_price)           AS total_revenue,
    COUNT(*)                        AS total_orders
FROM mock_data
GROUP BY store_country, store_city, store_state
ORDER BY store_country, total_revenue DESC;

-- Средний чек для каждого магазина
CREATE VIEW report_avg_order_per_store AS
SELECT
    store_name,
    store_city,
    store_country,
    COUNT(*)                        AS total_orders,
    SUM(sale_total_price)           AS total_revenue,
    ROUND(AVG(sale_total_price), 2) AS avg_order_value
FROM mock_data
GROUP BY store_name, store_city, store_country
ORDER BY avg_order_value DESC;


-- Топ-5 поставщиков по выручке
CREATE VIEW report_top5_suppliers_by_revenue AS
SELECT
    supplier_name,
    supplier_country,
    supplier_city,
    SUM(sale_total_price) AS total_revenue,
    COUNT(*)              AS total_orders
FROM mock_data
GROUP BY supplier_name, supplier_country, supplier_city
ORDER BY total_revenue DESC
LIMIT 5;

-- Средняя цена товаров от каждого поставщика
CREATE VIEW report_avg_price_by_supplier AS
SELECT
    supplier_name,
    supplier_country,
    COUNT(DISTINCT product_name)    AS unique_products,
    ROUND(AVG(product_price), 2)    AS avg_product_price,
    MIN(product_price)              AS min_price,
    MAX(product_price)              AS max_price
FROM mock_data
GROUP BY supplier_name, supplier_country
ORDER BY avg_product_price DESC;

-- Распределение продаж по странам поставщиков
CREATE VIEW report_sales_by_supplier_country AS
SELECT
    supplier_country,
    COUNT(DISTINCT supplier_name)   AS supplier_count,
    SUM(sale_total_price)           AS total_revenue,
    COUNT(*)                        AS total_orders
FROM mock_data
GROUP BY supplier_country
ORDER BY total_revenue DESC;


-- Продукты с наивысшим и наименьшим рейтингом
CREATE VIEW report_products_by_rating AS
SELECT
    product_name,
    product_category,
    product_brand,
    ROUND(AVG(product_rating), 2) AS avg_rating,
    SUM(product_reviews)          AS total_reviews,
    SUM(sale_quantity)            AS total_sold
FROM mock_data
GROUP BY product_name, product_category, product_brand
ORDER BY avg_rating DESC;

-- Корреляция между рейтингом и объёмом продаж
CREATE VIEW report_rating_vs_sales AS
SELECT
    product_name,
    product_category,
    ROUND(AVG(product_rating), 2) AS avg_rating,
    SUM(sale_quantity)            AS total_quantity_sold,
    SUM(sale_total_price)         AS total_revenue,
    SUM(product_reviews)          AS total_reviews,
    CASE
        WHEN AVG(product_rating) >= 4.5 THEN '4.5-5.0'
        WHEN AVG(product_rating) >= 4.0 THEN '4.0-4.5'
        WHEN AVG(product_rating) >= 3.0 THEN '3.0-4.0'
        WHEN AVG(product_rating) >= 2.0 THEN '2.0-3.0'
        ELSE 'below 2.0'
    END AS rating_bucket
FROM mock_data
GROUP BY product_name, product_category
ORDER BY avg_rating DESC;

-- Продукты с наибольшим количеством отзывов
CREATE VIEW report_top_reviewed_products AS
SELECT
    product_name,
    product_category,
    product_brand,
    SUM(product_reviews)          AS total_reviews,
    ROUND(AVG(product_rating), 2) AS avg_rating,
    SUM(sale_quantity)            AS total_sold
FROM mock_data
GROUP BY product_name, product_category, product_brand
ORDER BY total_reviews DESC
LIMIT 20;
