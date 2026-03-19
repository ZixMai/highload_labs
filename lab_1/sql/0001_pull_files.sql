DROP TABLE IF EXISTS mock_data;

CREATE TABLE mock_data (
    id INTEGER,
    customer_first_name VARCHAR(255),
    customer_last_name VARCHAR(255),
    customer_age INTEGER,
    customer_email VARCHAR(255),
    customer_country VARCHAR(255),
    customer_postal_code VARCHAR(50),
    customer_pet_type VARCHAR(100),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100),
    seller_first_name VARCHAR(255),
    seller_last_name VARCHAR(255),
    seller_email VARCHAR(255),
    seller_country VARCHAR(255),
    seller_postal_code VARCHAR(50),
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    product_price NUMERIC(10,2),
    product_quantity INTEGER,
    sale_date VARCHAR(50),
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12,2),
    store_name VARCHAR(255),
    store_location VARCHAR(255),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_country VARCHAR(255),
    store_phone VARCHAR(50),
    store_email VARCHAR(255),
    pet_category VARCHAR(100),
    product_weight NUMERIC(8,2),
    product_color VARCHAR(100),
    product_size VARCHAR(50),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_description TEXT,
    product_rating NUMERIC(3,2),
    product_reviews INTEGER,
    product_release_date VARCHAR(50),
    product_expiry_date VARCHAR(50),
    supplier_name VARCHAR(255),
    supplier_contact VARCHAR(255),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(50),
    supplier_address TEXT,
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(255)
);

CREATE TABLE load_batches (
    id bigint generated always as identity PRIMARY KEY,
    source_file text,
    load_txid xid8 NOT NULL,
    loaded_at timestamp NOT NULL DEFAULT now()
);

ALTER TABLE mock_data
    ADD COLUMN load_batch_id bigint;

ALTER TABLE mock_data
    ADD CONSTRAINT fk_mock_data_load_batch
    FOREIGN KEY (load_batch_id) REFERENCES load_batches(id);

CREATE OR REPLACE FUNCTION mock_data_attach_load_batch()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id bigint;
    v_source_file text;
BEGIN
    v_batch_id := nullif(current_setting('etl.load_batch_id', true), '')::bigint;

    IF v_batch_id IS NULL THEN
        v_source_file := nullif(current_setting('etl.source_file', true), '');

        INSERT INTO load_batches (source_file, load_txid)
        VALUES (v_source_file, pg_current_xact_id())
        RETURNING id INTO v_batch_id;

        PERFORM set_config('etl.load_batch_id', v_batch_id::text, true);
    END IF;

    NEW.load_batch_id := v_batch_id;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_mock_data_attach_load_batch ON mock_data;

CREATE TRIGGER trg_mock_data_attach_load_batch
BEFORE INSERT ON mock_data
FOR EACH ROW
EXECUTE FUNCTION mock_data_attach_load_batch();


CREATE OR REPLACE PROCEDURE load_mock_data(p_path text)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_file_name text;
BEGIN
    v_file_name := regexp_replace(p_path, '^.*/', '');

    PERFORM set_config('etl.source_file', v_file_name, true);
    PERFORM set_config('etl.load_batch_id', '', true);

    EXECUTE format(
        $sql$
        COPY mock_data (
            id,
            customer_first_name,
            customer_last_name,
            customer_age,
            customer_email,
            customer_country,
            customer_postal_code,
            customer_pet_type,
            customer_pet_name,
            customer_pet_breed,
            seller_first_name,
            seller_last_name,
            seller_email,
            seller_country,
            seller_postal_code,
            product_name,
            product_category,
            product_price,
            product_quantity,
            sale_date,
            sale_customer_id,
            sale_seller_id,
            sale_product_id,
            sale_quantity,
            sale_total_price,
            store_name,
            store_location,
            store_city,
            store_state,
            store_country,
            store_phone,
            store_email,
            pet_category,
            product_weight,
            product_color,
            product_size,
            product_brand,
            product_material,
            product_description,
            product_rating,
            product_reviews,
            product_release_date,
            product_expiry_date,
            supplier_name,
            supplier_contact,
            supplier_email,
            supplier_phone,
            supplier_address,
            supplier_city,
            supplier_country
        )
        FROM %L
        DELIMITER ','
        CSV HEADER
        NULL AS ''
        $sql$,
        p_path
    );
END;
$proc$;


CALL load_mock_data('/mock_data/MOCK_DATA.csv');
CALL load_mock_data('/mock_data/MOCK_DATA (1).csv');
CALL load_mock_data('/mock_data/MOCK_DATA (2).csv');
CALL load_mock_data('/mock_data/MOCK_DATA (3).csv');
CALL load_mock_data('/mock_data/MOCK_DATA (4).csv');
CALL load_mock_data('/mock_data/MOCK_DATA (5).csv');
CALL load_mock_data('/mock_data/MOCK_DATA (6).csv');
CALL load_mock_data('/mock_data/MOCK_DATA (7).csv');
CALL load_mock_data('/mock_data/MOCK_DATA (8).csv');
CALL load_mock_data('/mock_data/MOCK_DATA (9).csv');

SELECT 'rows: ' || COUNT(*) AS status FROM mock_data;