-- Star schema for streaming sales lab (Flink → PostgreSQL)

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year_num        SMALLINT NOT NULL,
    month_num       SMALLINT NOT NULL,
    day_num         SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id             INTEGER PRIMARY KEY,
    first_name              TEXT,
    last_name               TEXT,
    age                     INTEGER,
    email                   TEXT,
    country                 TEXT,
    postal_code             TEXT,
    pet_type                TEXT,
    pet_name                TEXT,
    pet_breed               TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id       INTEGER PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    email           TEXT,
    country         TEXT,
    postal_code     TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id          INTEGER PRIMARY KEY,
    product_name        TEXT,
    product_category    TEXT,
    product_price       DOUBLE PRECISION,
    product_quantity    INTEGER,
    pet_category        TEXT,
    product_weight      DOUBLE PRECISION,
    product_color       TEXT,
    product_size        TEXT,
    product_brand       TEXT,
    product_material    TEXT,
    product_description TEXT,
    product_rating      DOUBLE PRECISION,
    product_reviews     BIGINT,
    product_release_date TEXT,
    product_expiry_date  TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key           VARCHAR(512) PRIMARY KEY,
    store_name          TEXT,
    store_location      TEXT,
    store_city          TEXT,
    store_state         TEXT,
    store_country       TEXT,
    store_phone         TEXT,
    store_email         TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_email      VARCHAR(512) PRIMARY KEY,
    supplier_name       TEXT,
    supplier_contact    TEXT,
    supplier_phone      TEXT,
    supplier_address    TEXT,
    supplier_city       TEXT,
    supplier_country    TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id             BIGINT PRIMARY KEY,
    date_key            INTEGER NOT NULL REFERENCES dim_date (date_key),
    customer_id         INTEGER NOT NULL REFERENCES dim_customer (customer_id),
    seller_id           INTEGER NOT NULL REFERENCES dim_seller (seller_id),
    product_id          INTEGER NOT NULL REFERENCES dim_product (product_id),
    store_key           VARCHAR(512) NOT NULL REFERENCES dim_store (store_key),
    supplier_email      VARCHAR(512) NOT NULL REFERENCES dim_supplier (supplier_email),
    sale_quantity       INTEGER,
    sale_total_price    NUMERIC(14, 2)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales (customer_id);
