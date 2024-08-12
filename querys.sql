--- Checking Data Types ---
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'public' AND 
table_name = 'orders_table';


--- Find MAX Length for Columns ---
SELECT 
    MAX(LENGTH(card_number)) AS max_length_card_number,
    MAX(LENGTH(store_code)) AS max_length_store_code,
    MAX(LENGTH(product_code)) AS max_length_product_code
FROM orders_table;

SELECT 
    MAX(LENGTH(country_code)) AS max_country_code
FROM dim_users;

SELECT 
    MAX(LENGTH(store_code)) AS max_store_code,
	MAX(LENGTH(country_code)) AS max_country_code
FROM dim_store_details;

SELECT 
    MAX(LENGTH("EAN")) AS max_EAN,
	MAX(LENGTH(product_code)) AS max_product_code,
	MAX(LENGTH(weight_class)) AS max_weight_class
FROM dim_products;

SELECT 
    MAX(LENGTH(month)) AS max_month,
	MAX(LENGTH(year)) AS max_year,
	MAX(LENGTH(day)) AS max_day,
	MAX(LENGTH(time_period)) AS max_time_period
FROM dim_date_times;

--- Find MAX Length for Columns & Changing D-Types ---

SELECT 
    MAX(LENGTH(CAST(card_number AS VARCHAR))) AS max_card_number,
    MAX(LENGTH(CAST(expiry_date AS VARCHAR))) AS max_expiry_date
FROM dim_card_details;


--- Changing Data Types --- 
ALTER TABLE orders_table
    ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::uuid,
    ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN card_number SET DATA TYPE VARCHAR(19) USING card_number::varchar,
    ALTER COLUMN store_code SET DATA TYPE VARCHAR(12) USING store_code::varchar,
    ALTER COLUMN product_code SET DATA TYPE VARCHAR USING(11) product_code::varchar,
    ALTER COLUMN product_quantity SET DATA TYPE SMALLINT USING product_quantity::smallint;

ALTER TABLE dim_users
    ALTER COLUMN first_name SET DATA TYPE VARCHAR(255) USING first_name::varchar,
    ALTER COLUMN last_name SET DATA TYPE VARCHAR(255) USING last_name::varchar,
    ALTER COLUMN date_of_birth SET DATA TYPE DATE USING date_of_birth::date,
    ALTER COLUMN country_code SET DATA TYPE VARCHAR(10) USING country_code::varchar,
    ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN join_date SET DATA TYPE DATE USING join_date::date;
    
ALTER TABLE dim_store_details
    ALTER COLUMN longitude SET DATA TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality SET DATA TYPE VARCHAR(255) USING locality::varchar,
    ALTER COLUMN store_code SET DATA TYPE VARCHAR(12) USING store_code::varchar,
    ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date SET DATA TYPE DATE USING opening_date::date,
    ALTER COLUMN store_type SET DATA TYPE VARCHAR(255) USING store_type::varchar,
    ALTER COLUMN latitude SET DATA TYPE FLOAT USING latitude::FLOAT,
    ALTER COLUMN country_code SET DATA TYPE VARCHAR(2) USING country_code::varchar,
    ALTER COLUMN continent SET DATA TYPE VARCHAR(255) USING continent::varchar,


ALTER TABLE dim_products
    ALTER COLUMN product_price SET DATA TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight SET DATA TYPE FLOAT USING weight::FLOAT,
    ALTER COLUMN "EAN" SET DATA TYPE VARCHAR(17) USING "EAN"::VARCHAR,
    ALTER COLUMN product_code SET DATA TYPE VARCHAR(11) USING product_code::VARCHAR,
    ALTER COLUMN date_added SET DATA TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid SET DATA TYPE UUID USING uuid::UUID,
    ALTER COLUMN still_available SET DATA TYPE BOOL USING still_available::BOOL,
    ALTER COLUMN weight_class SET DATA TYPE VARCHAR(14) USING weight_class::VARCHAR;

ALTER TABLE dim_date_times
    ALTER COLUMN month SET DATA TYPE VARCHAR(10) USING month::VARCHAR,
    ALTER COLUMN year SET DATA TYPE VARCHAR(10) USING year::VARCHAR,
    ALTER COLUMN day SET DATA TYPE VARCHAR(10) USING day::VARCHAR,
    ALTER COLUMN time_period SET DATA TYPE VARCHAR(10) USING time_period::VARCHAR,
    ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID;
    
ALTER TABLE dim_card_details
    ALTER COLUMN card_number SET DATA TYPE VARCHAR(19) USING card_number::VARCHAR,
    ALTER COLUMN expiry_date SET DATA TYPE VARCHAR(5) USING expiry_date::VARCHAR,
    ALTER COLUMN date_payment_confirmed SET DATA TYPE DATE USING date_payment_confirmed::DATE;
    
--- UPDATING COLUMN VALUES USING REPLACE ---
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '') 

--- CREATING AND REPLICATING NEW COLUMNS --- 

ALTER TABLE dim_products 
ADD COLUMN weight_class TEXT;

UPDATE dim_products
SET weight_class = weight;

--- UPDATING NUMERICAL VALUES TO CATEGORICAL --- 

UPDATE dim_products
SET weight_class = 
    CASE 
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
		WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
		WHEN weight >= 140 THEN 'Truck_Required'
        ELSE weight_class  -- This keeps the original value if none of the conditions are met
    END;

--- RENAMING COLUMNS ---

ALTER TABLE dim_products
    RENAME removed TO still_available;

--- UPDATING VALUES --- 

UPDATE dim_products
SET still_available = 
    CASE 
        WHEN still_available = "Removed" THEN false
        WHEN still_available = "Still_available" THEN true
        ELSE NULL  -- Or another default value, if appropriate
    END;


--- ADDING PRIMARY KEYS ---

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);


--- ADDING FOREIGN KEY ---

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);

