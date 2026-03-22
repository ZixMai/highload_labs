INSERT INTO dim_category (category, batch_id)
SELECT DISTINCT TRIM(pet_category), load_batch_id
FROM mock_data
WHERE pet_category IS NOT NULL AND pet_category != '';

INSERT INTO dim_pet (pet_type, pet_name, pet_breed, batch_id)
SELECT DISTINCT
    TRIM(md.customer_pet_type),
    TRIM(md.customer_pet_name),
    TRIM(md.customer_pet_breed),
    md.load_batch_id
FROM mock_data md
LEFT JOIN dim_category c
    ON TRIM(md.pet_category) = c.category
WHERE md.customer_pet_type IS NOT NULL
  AND md.customer_pet_type != '';

INSERT INTO dim_customer (
    customer_first_name, customer_last_name, customer_age, customer_email, customer_country, customer_postal_code, customer_pet_id, batch_id, source_id
)
SELECT DISTINCT
    TRIM(md.customer_first_name),
    TRIM(md.customer_last_name),
    md.customer_age,
    TRIM(md.customer_email),
    TRIM(md.customer_country),
    TRIM(md.customer_postal_code),
    p.id,
    md.load_batch_id,
    md.sale_customer_id
FROM mock_data md
LEFT JOIN dim_pet p
    ON TRIM(md.customer_pet_type) = p.pet_type
    AND TRIM(md.customer_pet_name) = p.pet_name
    AND TRIM(md.customer_pet_breed) = p.pet_breed
    AND md.load_batch_id = p.batch_id
WHERE md.sale_customer_id IS NOT NULL;

INSERT INTO dim_product (
    product_name, product_category, product_price, product_quantity, product_weight, product_color, product_size,
    product_brand, product_material, product_description, product_rating, product_reviews, product_release_date,
    product_expiry_date, batch_id, source_id
)
SELECT DISTINCT
    TRIM(md.product_name),
    TRIM(md.product_category),
    md.product_price,
    md.product_quantity,
    md.product_weight,
    TRIM(md.product_color),
    TRIM(md.product_size),
    TRIM(md.product_brand),
    TRIM(md.product_material),
    TRIM(md.product_description),
    md.product_rating,
    md.product_reviews,
    CASE
        WHEN md.product_release_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        THEN TO_DATE(md.product_release_date, 'MM/DD/YYYY')
    END,
    CASE
        WHEN md.product_expiry_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        THEN TO_DATE(md.product_expiry_date, 'MM/DD/YYYY')
    END,
    md.load_batch_id,
    md.sale_product_id
FROM mock_data md
WHERE md.sale_product_id IS NOT NULL;

INSERT INTO dim_seller (
    seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code, batch_id, source_id
)
SELECT DISTINCT
    TRIM(md.seller_first_name),
    TRIM(md.seller_last_name),
    TRIM(md.seller_email),
    TRIM(md.seller_country),
    TRIM(md.seller_postal_code),
    md.load_batch_id,
    md.sale_seller_id
FROM mock_data md
WHERE md.sale_seller_id IS NOT NULL;

INSERT INTO dim_store (
    store_name, store_location, store_city, store_state, store_country, store_phone, store_email, batch_id
)
SELECT DISTINCT
    TRIM(md.store_name),
    TRIM(md.store_location),
    TRIM(md.store_city),
    TRIM(md.store_state),
    TRIM(md.store_country),
    TRIM(md.store_phone),
    TRIM(md.store_email),
    md.load_batch_id
FROM mock_data md
WHERE md.store_name IS NOT NULL;

INSERT INTO dim_supplier (
    supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country, batch_id
)
SELECT DISTINCT
    TRIM(md.supplier_name),
    TRIM(md.supplier_contact),
    TRIM(md.supplier_email),
    TRIM(md.supplier_phone),
    TRIM(md.supplier_address),
    TRIM(md.supplier_city),
    TRIM(md.supplier_country),
    md.load_batch_id
FROM mock_data md
WHERE md.supplier_name IS NOT NULL;

INSERT INTO fact_sale (
    sale_date, sale_customer_id, sale_seller_id, sale_product_id, sale_quantity, sale_total_price, sale_supplier_id, sale_store_id, sale_category_id
)
SELECT DISTINCT
    md.sale_date,
    c.id,
    s.id,
    p.id,
    md.sale_quantity,
    md.sale_total_price,
    sup.id,
    st.id,
    cat.id
FROM mock_data md
LEFT JOIN dim_category cat
    ON cat.category = TRIM(md.pet_category) AND cat.batch_id = md.load_batch_id
LEFT JOIN dim_customer c
    ON c.source_id = md.sale_customer_id AND c.batch_id = md.load_batch_id
LEFT JOIN dim_seller s
    ON s.source_id = md.sale_seller_id AND s.batch_id = md.load_batch_id
LEFT JOIN dim_product p
    ON p.source_id = md.sale_product_id AND p.batch_id = md.load_batch_id
LEFT JOIN LATERAL (
    SELECT st.id
    FROM dim_store st
    WHERE st.store_name = TRIM(md.store_name)
      AND st.store_email = TRIM(md.store_email)
      AND st.batch_id = md.load_batch_id
    ORDER BY st.id
    LIMIT 1
) st ON TRUE
LEFT JOIN LATERAL (
    SELECT sup.id
    FROM dim_supplier sup
    WHERE sup.supplier_name = TRIM(md.supplier_name)
      AND sup.supplier_email = TRIM(md.supplier_email)
      AND sup.batch_id = md.load_batch_id
    ORDER BY sup.id
    LIMIT 1
) sup ON TRUE;

-- Для соответствия запросу лабы:
DROP TABLE load_batches CASCADE;
ALTER TABLE "dim_store"    DROP COLUMN "batch_id";
ALTER TABLE "dim_supplier" DROP COLUMN "batch_id";
ALTER TABLE "dim_seller"   DROP COLUMN "batch_id";
ALTER TABLE "dim_customer" DROP COLUMN "batch_id";
ALTER TABLE "dim_product"  DROP COLUMN "batch_id";
ALTER TABLE "dim_pet"      DROP COLUMN "batch_id";
ALTER TABLE "dim_category" DROP COLUMN "batch_id";