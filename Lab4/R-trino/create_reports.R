#!/usr/bin/env Rscript

# Скрипт создаёт витрины-отчёты в ClickHouse через Trino (RPresto)
# Результаты будут помещены в схему lab4_reports в ClickHouse (Trino catalog 'clickhouse')

library(DBI)
library(RPresto)

trino_host <- Sys.getenv("TRINO_HOST", "localhost")
trino_port <- as.integer(Sys.getenv("TRINO_PORT", "8080"))
trino_user <- Sys.getenv("TRINO_USER", "trino")
trino_password <- Sys.getenv("TRINO_PASSWORD", "trino")

catalog <- Sys.getenv("TRINO_CLICKHOUSE_CATALOG", "clickhouse")
schema <- Sys.getenv("REPORT_SCHEMA", "lab4_reports")

conn <- dbConnect(
  Presto(),
  host = trino_host,
  port = trino_port,
  user = trino_user,
  # указываем catalog/schema (обязательно для PrestoDriver) и используем заголовки Trino
  catalog = catalog,
  schema = schema,
  use.trino.headers = TRUE
)

on.exit(dbDisconnect(conn), add = TRUE)

run <- function(sql) {
  cat("----------------------------------------\n")
  cat("SQL:\n", substring(sql, 1, 400), if (nchar(sql) > 400) "...\n")
  cat("Executing...\n")
  tryCatch({
    DBI::dbExecute(conn, sql)
    cat("OK\n")
  }, error = function(e) {
    cat("ERROR:\n")
    cat(conditionMessage(e), "\n")
  })
}

# Создать схему (база данных) если не существует
run(sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", catalog, schema))

# 1) Витрина продаж по продуктам
# product_sales: суммарная выручка, количество проданных штук, число продаж
run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.product_sales AS
SELECT p.id AS product_id,
       p.product_name,
       p.product_category,
       sum(f.sale_total_price) AS total_revenue,
       sum(f.sale_quantity) AS total_quantity,
       count() AS sales_count,
       avg(p.product_price) AS avg_list_price,
       max(p.product_rating) AS product_rating,
       max(p.product_reviews) AS product_reviews
FROM %3$s.lab4.fact_sale f
LEFT JOIN %3$s.lab4.dim_product p ON f.sale_product_id = p.id
GROUP BY p.id, p.product_name, p.product_category", catalog, schema, catalog))

# top-10 самых продаваемых продуктов
run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.top_10_products AS
SELECT product_id, product_name, product_category, total_revenue, total_quantity
FROM %1$s.%2$s_product_sales
ORDER BY total_quantity DESC
LIMIT 10", catalog, schema))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.top_10_products AS
SELECT product_id, product_name, product_category, total_revenue, total_quantity
FROM %1$s.%2$s.product_sales
ORDER BY total_quantity DESC
LIMIT 10", catalog, schema))

# общая выручка по категориям продуктов
run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.revenue_by_category AS
SELECT product_category, sum(total_revenue) AS revenue, sum(total_quantity) AS total_quantity
FROM %1$s.%2$s.product_sales
GROUP BY product_category
ORDER BY revenue DESC", catalog, schema))

# средний рейтинг и количество отзывов для каждого продукта
run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.product_rating_stats AS
SELECT p.id AS product_id, p.product_name, avg(p.product_rating) AS avg_rating, max(p.product_reviews) AS reviews_count
FROM %3$s.lab4.dim_product p
GROUP BY p.id, p.product_name", catalog, schema, catalog))

# 2) Витрина продаж по клиентам
run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.top_10_customers AS
SELECT f.sale_customer_id AS customer_id, c.customer_first_name, c.customer_last_name, sum(f.sale_total_price) AS total_spent
FROM %3$s.lab4.fact_sale f
LEFT JOIN %3$s.lab4.dim_customer c ON f.sale_customer_id = c.id
GROUP BY f.sale_customer_id, c.customer_first_name, c.customer_last_name
ORDER BY total_spent DESC
LIMIT 10", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.customers_by_country AS
SELECT c.customer_country, count(DISTINCT c.id) AS customers_count, sum(f.sale_total_price) AS revenue
FROM %3$s.lab4.dim_customer c
LEFT JOIN %3$s.lab4.fact_sale f ON f.sale_customer_id = c.id
GROUP BY c.customer_country
ORDER BY customers_count DESC", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.avg_check_by_customer AS
SELECT f.sale_customer_id AS customer_id, c.customer_first_name, c.customer_last_name, avg(f.sale_total_price) AS avg_check
FROM %3$s.lab4.fact_sale f
LEFT JOIN %3$s.lab4.dim_customer c ON f.sale_customer_id = c.id
GROUP BY f.sale_customer_id, c.customer_first_name, c.customer_last_name", catalog, schema, catalog))

# 3) Витрина продаж по времени
run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.sales_trends_monthly AS
SELECT date_trunc('month', CAST(concat(
    regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 3), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 1), 2, '0'), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 2), 2, '0')
 ) AS date) ) AS month,
    sum(f.sale_total_price) AS revenue,
    sum(f.sale_quantity) AS total_quantity,
    count() AS orders_count
FROM %3$s.lab4.fact_sale f
GROUP BY date_trunc('month', CAST(concat(
    regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 3), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 1), 2, '0'), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 2), 2, '0')
 ) AS date) )
ORDER BY month", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.sales_trends_yearly AS
SELECT date_trunc('year', CAST(concat(
    regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 3), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 1), 2, '0'), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 2), 2, '0')
 ) AS date) ) AS year,
    sum(f.sale_total_price) AS revenue,
    sum(f.sale_quantity) AS total_quantity,
    count() AS orders_count
FROM %3$s.lab4.fact_sale f
GROUP BY date_trunc('year', CAST(concat(
    regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 3), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 1), 2, '0'), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 2), 2, '0')
 ) AS date) )
ORDER BY year", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.avg_order_monthly AS
SELECT date_trunc('month', CAST(concat(
    regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 3), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 1), 2, '0'), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 2), 2, '0')
 ) AS date) ) AS month,
    avg(f.sale_total_price) AS avg_order_value
FROM %3$s.lab4.fact_sale f
GROUP BY date_trunc('month', CAST(concat(
    regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 3), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 1), 2, '0'), '-',
    lpad(regexp_extract(f.sale_date, '([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})', 2), 2, '0')
 ) AS date) )
ORDER BY month", catalog, schema, catalog))

# 4) Витрина продаж по магазинам
run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.top_5_stores AS
SELECT f.sale_store_id AS store_id, s.store_name, sum(f.sale_total_price) AS revenue
FROM %3$s.lab4.fact_sale f
LEFT JOIN %3$s.lab4.dim_store s ON f.sale_store_id = s.id
GROUP BY f.sale_store_id, s.store_name
ORDER BY revenue DESC
LIMIT 5", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.sales_by_city_country AS
SELECT s.store_city, s.store_country, sum(f.sale_total_price) AS revenue, sum(f.sale_quantity) AS total_quantity
FROM %3$s.lab4.fact_sale f
LEFT JOIN %3$s.lab4.dim_store s ON f.sale_store_id = s.id
GROUP BY s.store_city, s.store_country
ORDER BY revenue DESC", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.avg_check_by_store AS
SELECT f.sale_store_id AS store_id, s.store_name, avg(f.sale_total_price) AS avg_check
FROM %3$s.lab4.fact_sale f
LEFT JOIN %3$s.lab4.dim_store s ON f.sale_store_id = s.id
GROUP BY f.sale_store_id, s.store_name", catalog, schema, catalog))

# 5) Витрина продаж по поставщикам
run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.top_5_suppliers AS
SELECT f.sale_supplier_id AS supplier_id, sup.supplier_name, sum(f.sale_total_price) AS revenue
FROM %3$s.lab4.fact_sale f
LEFT JOIN %3$s.lab4.dim_supplier sup ON f.sale_supplier_id = sup.id
GROUP BY f.sale_supplier_id, sup.supplier_name
ORDER BY revenue DESC
LIMIT 5", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.avg_price_by_supplier AS
SELECT sup.id AS supplier_id, sup.supplier_name, avg(p.product_price) AS avg_price
FROM %3$s.lab4.dim_supplier sup
LEFT JOIN %3$s.lab4.dim_product p ON p.source_file = sup.source_file -- best-effort join
GROUP BY sup.id, sup.supplier_name", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.sales_by_supplier_country AS
SELECT sup.supplier_country, sum(f.sale_total_price) AS revenue, sum(f.sale_quantity) AS total_quantity
FROM %3$s.lab4.fact_sale f
LEFT JOIN %3$s.lab4.dim_supplier sup ON f.sale_supplier_id = sup.id
GROUP BY sup.supplier_country
ORDER BY revenue DESC", catalog, schema, catalog))

# 6) Витрина качества продукции
run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.product_rating_extremes AS
SELECT id AS product_id, product_name, product_rating
FROM %3$s.lab4.dim_product
WHERE product_rating IS NOT NULL
ORDER BY product_rating DESC
LIMIT 10", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.rating_sales_stats AS
SELECT p.id AS product_id, p.product_name, p.product_rating, sum(f.sale_quantity) AS total_quantity, sum(f.sale_total_price) AS total_revenue
FROM %3$s.lab4.dim_product p
LEFT JOIN %3$s.lab4.fact_sale f ON f.sale_product_id = p.id
GROUP BY p.id, p.product_name, p.product_rating", catalog, schema, catalog))

run(sprintf("CREATE TABLE IF NOT EXISTS %1$s.%2$s.top_products_by_reviews AS
SELECT id AS product_id, product_name, product_reviews
FROM %3$s.lab4.dim_product
ORDER BY product_reviews DESC
LIMIT 20", catalog, schema, catalog))

cat("All reports executed (see messages above).\n")





