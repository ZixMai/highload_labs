CREATE DATABASE IF NOT EXISTS lab4;

DROP TABLE IF EXISTS lab4.mock_data;

CREATE TABLE lab4.mock_data
(
    id                   Int32,
    customer_first_name  String,
    customer_last_name   String,
    customer_age         Int32,
    customer_email       String,
    customer_country     String,
    customer_postal_code String,
    customer_pet_type    String,
    customer_pet_name    String,
    customer_pet_breed   String,
    seller_first_name    String,
    seller_last_name     String,
    seller_email         String,
    seller_country       String,
    seller_postal_code   String,
    product_name         String,
    product_category     String,
    product_price        Decimal(10, 2),
    product_quantity     Int32,
    sale_date            String,
    sale_customer_id     Int32,
    sale_seller_id       Int32,
    sale_product_id      Int32,
    sale_quantity        Int32,
    sale_total_price     Decimal(12, 2),
    store_name           String,
    store_location       String,
    store_city           String,
    store_state          String,
    store_country        String,
    store_phone          String,
    store_email          String,
    pet_category         String,
    product_weight       Decimal(8, 2),
    product_color        String,
    product_size         String,
    product_brand        String,
    product_material     String,
    product_description  String,
    product_rating       Decimal(3, 2),
    product_reviews      Int32,
    product_release_date String,
    product_expiry_date  String,
    supplier_name        String,
    supplier_contact     String,
    supplier_email       String,
    supplier_phone       String,
    supplier_address     String,
    supplier_city        String,
    supplier_country     String,
    source_file          String
)
ENGINE = MergeTree()
ORDER BY id;


INSERT INTO lab4.mock_data
SELECT
    id, customer_first_name, customer_last_name, customer_age, customer_email,
    customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
    customer_pet_breed, seller_first_name, seller_last_name, seller_email,
    seller_country, seller_postal_code, product_name, product_category,
    product_price, product_quantity, sale_date, sale_customer_id, sale_seller_id,
    sale_product_id, sale_quantity, sale_total_price, store_name, store_location,
    store_city, store_state, store_country, store_phone, store_email, pet_category,
    product_weight, product_color, product_size, product_brand, product_material,
    product_description, product_rating, product_reviews, product_release_date,
    product_expiry_date, supplier_name, supplier_contact, supplier_email,
    supplier_phone, supplier_address, supplier_city, supplier_country,
    '/mock_data/MOCK_DATA (5).csv' AS source_file
FROM file('/mock_data/MOCK_DATA (5).csv', CSVWithNames);

INSERT INTO lab4.mock_data
SELECT
    id, customer_first_name, customer_last_name, customer_age, customer_email,
    customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
    customer_pet_breed, seller_first_name, seller_last_name, seller_email,
    seller_country, seller_postal_code, product_name, product_category,
    product_price, product_quantity, sale_date, sale_customer_id, sale_seller_id,
    sale_product_id, sale_quantity, sale_total_price, store_name, store_location,
    store_city, store_state, store_country, store_phone, store_email, pet_category,
    product_weight, product_color, product_size, product_brand, product_material,
    product_description, product_rating, product_reviews, product_release_date,
    product_expiry_date, supplier_name, supplier_contact, supplier_email,
    supplier_phone, supplier_address, supplier_city, supplier_country,
    '/mock_data/MOCK_DATA (6).csv' AS source_file
FROM file('/mock_data/MOCK_DATA (6).csv', CSVWithNames);

INSERT INTO lab4.mock_data
SELECT
    id, customer_first_name, customer_last_name, customer_age, customer_email,
    customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
    customer_pet_breed, seller_first_name, seller_last_name, seller_email,
    seller_country, seller_postal_code, product_name, product_category,
    product_price, product_quantity, sale_date, sale_customer_id, sale_seller_id,
    sale_product_id, sale_quantity, sale_total_price, store_name, store_location,
    store_city, store_state, store_country, store_phone, store_email, pet_category,
    product_weight, product_color, product_size, product_brand, product_material,
    product_description, product_rating, product_reviews, product_release_date,
    product_expiry_date, supplier_name, supplier_contact, supplier_email,
    supplier_phone, supplier_address, supplier_city, supplier_country,
    '/mock_data/MOCK_DATA (7).csv' AS source_file
FROM file('/mock_data/MOCK_DATA (7).csv', CSVWithNames);

INSERT INTO lab4.mock_data
SELECT
    id, customer_first_name, customer_last_name, customer_age, customer_email,
    customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
    customer_pet_breed, seller_first_name, seller_last_name, seller_email,
    seller_country, seller_postal_code, product_name, product_category,
    product_price, product_quantity, sale_date, sale_customer_id, sale_seller_id,
    sale_product_id, sale_quantity, sale_total_price, store_name, store_location,
    store_city, store_state, store_country, store_phone, store_email, pet_category,
    product_weight, product_color, product_size, product_brand, product_material,
    product_description, product_rating, product_reviews, product_release_date,
    product_expiry_date, supplier_name, supplier_contact, supplier_email,
    supplier_phone, supplier_address, supplier_city, supplier_country,
    '/mock_data/MOCK_DATA (8).csv' AS source_file
FROM file('/mock_data/MOCK_DATA (8).csv', CSVWithNames);

INSERT INTO lab4.mock_data
SELECT
    id, customer_first_name, customer_last_name, customer_age, customer_email,
    customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
    customer_pet_breed, seller_first_name, seller_last_name, seller_email,
    seller_country, seller_postal_code, product_name, product_category,
    product_price, product_quantity, sale_date, sale_customer_id, sale_seller_id,
    sale_product_id, sale_quantity, sale_total_price, store_name, store_location,
    store_city, store_state, store_country, store_phone, store_email, pet_category,
    product_weight, product_color, product_size, product_brand, product_material,
    product_description, product_rating, product_reviews, product_release_date,
    product_expiry_date, supplier_name, supplier_contact, supplier_email,
    supplier_phone, supplier_address, supplier_city, supplier_country,
    '/mock_data/MOCK_DATA (9).csv' AS source_file
FROM file('/mock_data/MOCK_DATA (9).csv', CSVWithNames);


SELECT 'rows: ' || toString(COUNT(*)) AS status
FROM lab4.mock_data;

DROP TABLE IF EXISTS lab4.dim_store;
CREATE TABLE lab4.dim_store
(
    id             UInt64,
    store_name     String,
    store_location String,
    store_city     String,
    store_state    String,
    store_country  String,
    store_phone    String,
    store_email    String,
    source_file    String
)
    ENGINE = ReplacingMergeTree()
ORDER BY id;

DROP TABLE IF EXISTS lab4.dim_supplier;
CREATE TABLE lab4.dim_supplier
(
    id               UInt64,
    supplier_name    String,
    supplier_contact String,
    supplier_email   String,
    supplier_phone   String,
    supplier_address String,
    supplier_city    String,
    supplier_country String,
    source_file      String,
    source_id        UInt64
)
    ENGINE = ReplacingMergeTree()
ORDER BY id;

DROP TABLE IF EXISTS lab4.dim_seller;
CREATE TABLE lab4.dim_seller
(
    id                 UInt64,
    seller_first_name  String,
    seller_last_name   String,
    seller_email       String,
    seller_country     String,
    seller_postal_code String,
    source_file        String,
    source_id          UInt64
)
    ENGINE = ReplacingMergeTree()
ORDER BY id;

DROP TABLE IF EXISTS lab4.dim_pet;
CREATE TABLE lab4.dim_pet
(
    id        UInt64,
    pet_type  String,
    pet_name  String,
    pet_breed String,
    source_file String
)
    ENGINE = ReplacingMergeTree()
ORDER BY id;

DROP TABLE IF EXISTS lab4.dim_customer;
CREATE TABLE lab4.dim_customer
(
    id                   UInt64,
    customer_first_name  String,
    customer_last_name   String,
    customer_age         Int32,
    customer_email       String,
    customer_country     String,
    customer_postal_code String,
    customer_pet_id      UInt64,
    source_file          String,
    source_id            UInt64
)
    ENGINE = ReplacingMergeTree()
ORDER BY id;

DROP TABLE IF EXISTS lab4.dim_product;
CREATE TABLE lab4.dim_product
(
    id                   UInt64,
    product_name         String,
    product_category     String,
    product_price        Decimal(10, 2),
    product_quantity     Int32,
    product_weight       Decimal(8, 2),
    product_color        String,
    product_size         String,
    product_brand        String,
    product_material     String,
    product_description  String,
    product_rating       Decimal(3, 2),
    product_reviews      Int32,
    product_release_date String,
    product_expiry_date  String,
    source_file          String,
    source_id            UInt64
)
    ENGINE = ReplacingMergeTree()
ORDER BY id;

DROP TABLE IF EXISTS lab4.dim_category;
CREATE TABLE lab4.dim_category
(
    id          UInt64,
    category    String,
    source_file String
)
    ENGINE = ReplacingMergeTree()
ORDER BY id;


DROP TABLE IF EXISTS lab4.fact_sale;
CREATE TABLE lab4.fact_sale
(
    id               UInt64,
    sale_date        String,
    sale_customer_id UInt64,
    sale_seller_id   UInt64,
    sale_product_id  UInt64,
    sale_quantity    Int32,
    sale_total_price Decimal(12, 2),
    sale_supplier_id UInt64,
    sale_store_id    UInt64,
    sale_category_id UInt64,
    source_file      String
)
    ENGINE = MergeTree()
ORDER BY (sale_date, id);



ALTER TABLE lab4.dim_category
    ADD INDEX ix_dim_category_category_batch (category, source_file) TYPE bloom_filter GRANULARITY 1;

ALTER TABLE lab4.dim_pet
    ADD INDEX ix_dim_pet_type_name_breed (pet_type, pet_name, pet_breed, source_file) TYPE bloom_filter GRANULARITY 1;

ALTER TABLE lab4.dim_customer
    ADD INDEX ix_dim_customer_source_batch (source_id, source_file) TYPE minmax GRANULARITY 1;

ALTER TABLE lab4.dim_seller
    ADD INDEX ix_dim_seller_source_batch (source_id, source_file) TYPE minmax GRANULARITY 1;

ALTER TABLE lab4.dim_product
    ADD INDEX ix_dim_product_source_batch (source_id, source_file) TYPE minmax GRANULARITY 1;

ALTER TABLE lab4.dim_store
    ADD INDEX ix_dim_store_name_email_batch (store_email, source_file) TYPE bloom_filter GRANULARITY 1;

ALTER TABLE lab4.dim_supplier
    ADD INDEX ix_dim_supplier_source_batch (supplier_email, source_file) TYPE bloom_filter GRANULARITY 1;
