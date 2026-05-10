package app

import (
	"context"
	"database/sql"
	"fmt"
	"golang-trino/internal/trino_connector"
	"log/slog"
	"os"
)

func Run(ctx context.Context, cancel context.CancelFunc) {
	internalCtx := context.WithoutCancel(ctx)
	defer cancel()

	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(l)

	cfg, err := trino_connector.ConfigFromEnv()
	if err != nil {
		slog.ErrorContext(internalCtx, fmt.Sprintf("Failed to load Trino config: %s", err))
		return
	}

	trinoClient, err := trino_connector.GetTrinoClient(cfg)
	if err != nil {
		slog.ErrorContext(internalCtx, fmt.Sprintf("Failed to connect Trino: %s", err))
		return
	}
	defer func() {
		if err := trinoClient.Close(); err != nil {
			slog.ErrorContext(internalCtx, fmt.Sprintf("Failed to close Trino connection: %s", err))
		}
	}()

	if err := loadStarFromRaw(internalCtx, trinoClient); err != nil {
		slog.ErrorContext(internalCtx, fmt.Sprintf("Failed to load star schema: %s", err))
	}
}

func loadStarFromRaw(ctx context.Context, db *sql.DB) error {
	rawCTE := `
WITH raw AS (
    SELECT
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
        supplier_country,
        source_file
    FROM postgres.public.mock_data
    UNION ALL
    SELECT
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
        supplier_country,
        source_file
    FROM clickhouse.default.mock_data
)
`

	statements := []string{
		`INSERT INTO clickhouse.lab4.dim_pet (id, pet_type, pet_name, pet_breed, source_file)
` + rawCTE + `
SELECT DISTINCT
    abs(from_big_endian_64(xxhash64(to_utf8(concat(
        coalesce(customer_pet_type, ''), '|',
        coalesce(customer_pet_name, ''), '|',
        coalesce(customer_pet_breed, ''), '|',
        coalesce(source_file, '')
    ))))) AS id,
    coalesce(customer_pet_type, ''),
    coalesce(customer_pet_name, ''),
    coalesce(customer_pet_breed, ''),
    coalesce(source_file, '')
FROM (
    SELECT
        nullif(trim(customer_pet_type), '') AS customer_pet_type,
        nullif(trim(customer_pet_name), '') AS customer_pet_name,
        nullif(trim(customer_pet_breed), '') AS customer_pet_breed,
        source_file
    FROM raw
)
WHERE customer_pet_type IS NOT NULL
   OR customer_pet_name IS NOT NULL
   OR customer_pet_breed IS NOT NULL
`,
		`INSERT INTO clickhouse.lab4.dim_customer (
    id,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_id,
    source_file,
    source_id
)
` + rawCTE + `
SELECT DISTINCT
    abs(from_big_endian_64(xxhash64(to_utf8(concat(cast(sale_customer_id AS varchar), '|', source_file))))) AS id,
    coalesce(customer_first_name, ''),
    coalesce(customer_last_name, ''),
    coalesce(customer_age, 0),
    coalesce(customer_email, ''),
    coalesce(customer_country, ''),
    coalesce(customer_postal_code, ''),
    coalesce(CASE
        WHEN customer_pet_type IS NOT NULL
          OR customer_pet_name IS NOT NULL
          OR customer_pet_breed IS NOT NULL
        THEN abs(from_big_endian_64(xxhash64(to_utf8(concat(
            coalesce(customer_pet_type, ''), '|',
            coalesce(customer_pet_name, ''), '|',
            coalesce(customer_pet_breed, ''), '|',
            coalesce(source_file, '')
        )))))
        ELSE NULL
    END, 0) AS customer_pet_id,
    coalesce(source_file, ''),
    sale_customer_id AS source_id
FROM (
    SELECT
        sale_customer_id,
        nullif(trim(customer_first_name), '') AS customer_first_name,
        nullif(trim(customer_last_name), '') AS customer_last_name,
        customer_age,
        nullif(trim(customer_email), '') AS customer_email,
        nullif(trim(customer_country), '') AS customer_country,
        nullif(trim(customer_postal_code), '') AS customer_postal_code,
        nullif(trim(customer_pet_type), '') AS customer_pet_type,
        nullif(trim(customer_pet_name), '') AS customer_pet_name,
        nullif(trim(customer_pet_breed), '') AS customer_pet_breed,
        nullif(trim(source_file), '') AS source_file
    FROM raw
)
WHERE sale_customer_id IS NOT NULL
  AND source_file IS NOT NULL
`,
		`INSERT INTO clickhouse.lab4.dim_seller (
    id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code,
    source_file,
    source_id
)
` + rawCTE + `
SELECT DISTINCT
    abs(from_big_endian_64(xxhash64(to_utf8(concat(cast(sale_seller_id AS varchar), '|', source_file))))) AS id,
    coalesce(seller_first_name, ''),
    coalesce(seller_last_name, ''),
    coalesce(seller_email, ''),
    coalesce(seller_country, ''),
    coalesce(seller_postal_code, ''),
    coalesce(source_file, ''),
    sale_seller_id AS source_id
FROM (
    SELECT
        sale_seller_id,
        nullif(trim(seller_first_name), '') AS seller_first_name,
        nullif(trim(seller_last_name), '') AS seller_last_name,
        nullif(trim(seller_email), '') AS seller_email,
        nullif(trim(seller_country), '') AS seller_country,
        nullif(trim(seller_postal_code), '') AS seller_postal_code,
        nullif(trim(source_file), '') AS source_file
    FROM raw
)
WHERE sale_seller_id IS NOT NULL
  AND source_file IS NOT NULL
`,
		`INSERT INTO clickhouse.lab4.dim_supplier (
    id,
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country,
    source_file,
    source_id
)
` + rawCTE + `
SELECT DISTINCT
    abs(from_big_endian_64(xxhash64(to_utf8(concat(supplier_email, '|', source_file))))) AS id,
    coalesce(supplier_name, ''),
    coalesce(supplier_contact, ''),
    coalesce(supplier_email, ''),
    coalesce(supplier_phone, ''),
    coalesce(supplier_address, ''),
    coalesce(supplier_city, ''),
    coalesce(supplier_country, ''),
    coalesce(source_file, ''),
    abs(from_big_endian_64(xxhash64(to_utf8(concat(supplier_email, '|', source_file))))) AS source_id
FROM (
    SELECT
        nullif(trim(supplier_name), '') AS supplier_name,
        nullif(trim(supplier_contact), '') AS supplier_contact,
        nullif(trim(supplier_email), '') AS supplier_email,
        nullif(trim(supplier_phone), '') AS supplier_phone,
        nullif(trim(supplier_address), '') AS supplier_address,
        nullif(trim(supplier_city), '') AS supplier_city,
        nullif(trim(supplier_country), '') AS supplier_country,
        nullif(trim(source_file), '') AS source_file
    FROM raw
)
WHERE supplier_email IS NOT NULL
  AND source_file IS NOT NULL
`,
		`INSERT INTO clickhouse.lab4.dim_store (
    id,
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email,
    source_file
)
` + rawCTE + `
SELECT DISTINCT
    abs(from_big_endian_64(xxhash64(to_utf8(concat(store_email, '|', source_file))))) AS id,
    coalesce(store_name, ''),
    coalesce(store_location, ''),
    coalesce(store_city, ''),
    coalesce(store_state, ''),
    coalesce(store_country, ''),
    coalesce(store_phone, ''),
    coalesce(store_email, ''),
    coalesce(source_file, '')
FROM (
    SELECT
        nullif(trim(store_name), '') AS store_name,
        nullif(trim(store_location), '') AS store_location,
        nullif(trim(store_city), '') AS store_city,
        nullif(trim(store_state), '') AS store_state,
        nullif(trim(store_country), '') AS store_country,
        nullif(trim(store_phone), '') AS store_phone,
        nullif(trim(store_email), '') AS store_email,
        nullif(trim(source_file), '') AS source_file
    FROM raw
)
WHERE store_email IS NOT NULL
  AND source_file IS NOT NULL
`,
		`INSERT INTO clickhouse.lab4.dim_category (id, category, source_file)
` + rawCTE + `
SELECT DISTINCT
    abs(from_big_endian_64(xxhash64(to_utf8(concat(category, '|', source_file))))) AS id,
    coalesce(category, ''),
    coalesce(source_file, '')
FROM (
    SELECT
        nullif(trim(pet_category), '') AS category,
        nullif(trim(source_file), '') AS source_file
    FROM raw
)
WHERE category IS NOT NULL
`,
		`INSERT INTO clickhouse.lab4.dim_product (
    id,
    product_name,
    product_category,
    product_price,
    product_quantity,
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
    source_file,
    source_id
)
` + rawCTE + `
SELECT DISTINCT
    abs(from_big_endian_64(xxhash64(to_utf8(concat(cast(sale_product_id AS varchar), '|', source_file))))) AS id,
    coalesce(product_name, ''),
    coalesce(product_category, ''),
    coalesce(product_price, 0),
    coalesce(product_quantity, 0),
    coalesce(product_weight, 0),
    coalesce(product_color, ''),
    coalesce(product_size, ''),
    coalesce(product_brand, ''),
    coalesce(product_material, ''),
    coalesce(product_description, ''),
    coalesce(product_rating, 0),
    coalesce(product_reviews, 0),
    coalesce(product_release_date, ''),
    coalesce(product_expiry_date, ''),
    coalesce(source_file, ''),
    sale_product_id AS source_id
FROM (
    SELECT
        sale_product_id,
        nullif(trim(product_name), '') AS product_name,
        nullif(trim(product_category), '') AS product_category,
        product_price,
        product_quantity,
        product_weight,
        nullif(trim(product_color), '') AS product_color,
        nullif(trim(product_size), '') AS product_size,
        nullif(trim(product_brand), '') AS product_brand,
        nullif(trim(product_material), '') AS product_material,
        nullif(trim(product_description), '') AS product_description,
        product_rating,
        product_reviews,
        nullif(trim(product_release_date), '') AS product_release_date,
        nullif(trim(product_expiry_date), '') AS product_expiry_date,
        nullif(trim(source_file), '') AS source_file
    FROM raw
)
WHERE sale_product_id IS NOT NULL
  AND source_file IS NOT NULL
`,
		`INSERT INTO clickhouse.lab4.fact_sale (
    id,
    sale_date,
    sale_customer_id,
    sale_seller_id,
    sale_product_id,
    sale_quantity,
    sale_total_price,
    sale_supplier_id,
    sale_store_id,
    sale_category_id,
    source_file
)
` + rawCTE + `
SELECT
    abs(from_big_endian_64(xxhash64(to_utf8(concat(cast(id AS varchar), '|', coalesce(source_file, '')))))) AS id,
    coalesce(sale_date, ''),
    coalesce(CASE
        WHEN sale_customer_id IS NOT NULL AND source_file IS NOT NULL
        THEN abs(from_big_endian_64(xxhash64(to_utf8(concat(cast(sale_customer_id AS varchar), '|', source_file)))))
        ELSE NULL
    END, 0) AS sale_customer_id,
    coalesce(CASE
        WHEN sale_seller_id IS NOT NULL AND source_file IS NOT NULL
        THEN abs(from_big_endian_64(xxhash64(to_utf8(concat(cast(sale_seller_id AS varchar), '|', source_file)))))
        ELSE NULL
    END, 0) AS sale_seller_id,
    coalesce(CASE
        WHEN sale_product_id IS NOT NULL AND source_file IS NOT NULL
        THEN abs(from_big_endian_64(xxhash64(to_utf8(concat(cast(sale_product_id AS varchar), '|', source_file)))))
        ELSE NULL
    END, 0) AS sale_product_id,
    coalesce(sale_quantity, 0),
    coalesce(sale_total_price, 0),
    coalesce(CASE
        WHEN supplier_email IS NOT NULL AND source_file IS NOT NULL
        THEN abs(from_big_endian_64(xxhash64(to_utf8(concat(supplier_email, '|', source_file)))))
        ELSE NULL
    END, 0) AS sale_supplier_id,
    coalesce(CASE
        WHEN store_email IS NOT NULL AND source_file IS NOT NULL
        THEN abs(from_big_endian_64(xxhash64(to_utf8(concat(store_email, '|', source_file)))))
        ELSE NULL
    END, 0) AS sale_store_id,
    coalesce(CASE
        WHEN pet_category IS NOT NULL AND source_file IS NOT NULL
        THEN abs(from_big_endian_64(xxhash64(to_utf8(concat(pet_category, '|', source_file)))))
        ELSE NULL
    END, 0) AS sale_category_id,
    coalesce(source_file, '')
FROM (
    SELECT
        id,
        nullif(trim(sale_date), '') AS sale_date,
        sale_customer_id,
        sale_seller_id,
        sale_product_id,
        sale_quantity,
        sale_total_price,
        nullif(trim(supplier_email), '') AS supplier_email,
        nullif(trim(store_email), '') AS store_email,
        nullif(trim(pet_category), '') AS pet_category,
        nullif(trim(source_file), '') AS source_file
    FROM raw
)
`,
	}

	for i, stmt := range statements {
		slog.InfoContext(ctx, "Executing statement", "index", i+1)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute statement %d: %w", i+1, err)
		}
	}

	return nil
}
