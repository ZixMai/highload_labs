CREATE TABLE "dim_store" (
  "id" bigint generated always as identity PRIMARY KEY,
  "store_name" varchar(255),
  "store_location" varchar(255),
  "store_city" varchar(100),
  "store_state" varchar(100),
  "store_country" varchar(255),
  "store_phone" varchar(50),
  "store_email" varchar(255)
);

CREATE TABLE "dim_supplier" (
  "id" bigint generated always as identity PRIMARY KEY,
  "supplier_name" varchar(255),
  "supplier_contact" varchar(255),
  "supplier_email" varchar(255),
  "supplier_phone" varchar(50),
  "supplier_address" text,
  "supplier_city" varchar(100),
  "supplier_country" varchar(255)
);

CREATE TABLE "dim_seller" (
  "id" bigint generated always as identity PRIMARY KEY,
  "seller_first_name" varchar(255),
  "seller_last_name" varchar(255),
  "seller_email" varchar(255),
  "seller_country" varchar(255),
  "seller_postal_code" varchar(50)
);

CREATE TABLE "dim_customer" (
  "id" bigint generated always as identity PRIMARY KEY,
  "customer_first_name" varchar(255),
  "customer_last_name" varchar(255),
  "customer_age" int,
  "customer_email" varchar(255),
  "customer_country" varchar(255),
  "customer_postal_code" varchar(50),
  "customer_pet_id" bigint
);

CREATE TABLE "dim_product" (
  "id" bigint generated always as identity PRIMARY KEY,
  "product_name" varchar(255),
  "product_category" varchar(100),
  "product_price" numeric(10,2),
  "product_quantity" int,
  "product_weight" numeric(8,2),
  "product_color" varchar(100),
  "product_size" varchar(50),
  "product_brand" varchar(100),
  "product_material" varchar(100),
  "product_description" text,
  "product_rating" numeric(3,2),
  "product_reviews" int,
  "product_release_date" varchar(50),
  "product_expiry_date" varchar(50)
);

CREATE TABLE "dim_pet" (
  "id" bigint generated always as identity PRIMARY KEY,
  "pet_type" varchar(100),
  "pet_name" varchar(100),
  "pet_breed" varchar(100)
);

CREATE TABLE "dim_category" (
  "id" bigint generated always as identity PRIMARY KEY,
  "category" varchar(100)
);

CREATE TABLE "fact_sale" (
  "id" bigint generated always as identity PRIMARY KEY,
  "sale_date" varchar(50),
  "sale_customer_id" bigint,
  "sale_seller_id" bigint,
  "sale_product_id" bigint,
  "sale_quantity" int,
  "sale_total_price" numeric(12,2),
  "sale_supplier_id" bigint,
  "sale_store_id" bigint,
  "sale_category_id" bigint
);

ALTER TABLE "fact_sale" ADD FOREIGN KEY ("sale_product_id") REFERENCES "dim_product" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "fact_sale" ADD FOREIGN KEY ("sale_seller_id") REFERENCES "dim_seller" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "fact_sale" ADD FOREIGN KEY ("sale_customer_id") REFERENCES "dim_customer" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "fact_sale" ADD FOREIGN KEY ("sale_store_id") REFERENCES "dim_store" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "fact_sale" ADD FOREIGN KEY ("sale_supplier_id") REFERENCES "dim_supplier" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "fact_sale" ADD FOREIGN KEY ("sale_category_id") REFERENCES "dim_category" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "dim_customer" ADD FOREIGN KEY ("customer_pet_id") REFERENCES "dim_pet" ("id") DEFERRABLE INITIALLY IMMEDIATE;


ALTER TABLE "dim_store"
  ADD COLUMN "batch_id" bigint,
  ADD CONSTRAINT "fk_dim_store_batch"
    FOREIGN KEY ("batch_id") REFERENCES "load_batches" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "dim_supplier"
  ADD COLUMN "batch_id" bigint,
  ADD COLUMN "source_id" bigint,
  ADD CONSTRAINT "fk_dim_supplier_batch"
    FOREIGN KEY ("batch_id") REFERENCES "load_batches" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "dim_seller"
  ADD COLUMN "batch_id" bigint,
  ADD COLUMN "source_id" bigint,
  ADD CONSTRAINT "fk_dim_seller_batch"
    FOREIGN KEY ("batch_id") REFERENCES "load_batches" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "dim_customer"
  ADD COLUMN "batch_id" bigint,
  ADD COLUMN "source_id" bigint,
  ADD CONSTRAINT "fk_dim_customer_batch"
    FOREIGN KEY ("batch_id") REFERENCES "load_batches" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "dim_product"
  ADD COLUMN "batch_id" bigint,
  ADD COLUMN "source_id" bigint,
  ADD CONSTRAINT "fk_dim_product_batch"
    FOREIGN KEY ("batch_id") REFERENCES "load_batches" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "dim_pet"
  ADD COLUMN "batch_id" bigint,
  ADD CONSTRAINT "fk_dim_pet_batch"
    FOREIGN KEY ("batch_id") REFERENCES "load_batches" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "dim_category"
  ADD COLUMN "batch_id" bigint,
  ADD CONSTRAINT "fk_dim_category_batch"
    FOREIGN KEY ("batch_id") REFERENCES "load_batches" ("id") DEFERRABLE INITIALLY IMMEDIATE;


CREATE INDEX ix_dim_category_category_batch
    ON dim_category (category, batch_id);

CREATE INDEX ix_dim_pet_type_name_breed
    ON dim_pet (pet_type, pet_name, pet_breed, batch_id);

CREATE INDEX ix_dim_customer_source_batch
    ON dim_customer (source_id, batch_id);

CREATE INDEX ix_dim_seller_source_batch
    ON dim_seller (source_id, batch_id);

CREATE INDEX ix_dim_product_source_batch
    ON dim_product (source_id, batch_id);

CREATE INDEX ix_dim_store_name_email_batch
    ON dim_store (store_name, store_email, batch_id);

CREATE INDEX ix_dim_supplier_name_batch
    ON dim_supplier (supplier_name, batch_id);
