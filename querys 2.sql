--- GroupBy Country and Stores ---

SELECT 
    country_code, 
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_no_stores DESC;

SELECT 
    locality, 
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
ORDER BY 
    total_no_stores DESC
LIMIT 7;

--- CTE ---

WITH sales_data AS (
    SELECT 
		orders_table.date_uuid,
        dim_date_times.month,
        orders_table.product_quantity * dim_products.product_price AS sales
    FROM 
        orders_table
    JOIN 
        dim_products ON orders_table.product_code = dim_products.product_code
    JOIN 
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
)
SELECT 
    SUM(sales) AS total_sales,
    month
FROM 
    sales_data
GROUP BY 
    month
ORDER BY 
    total_sales DESC
LIMIT 
	6;


--- ADD NEW COLUMN WITH CAST ---

ALTER TABLE dim_store_details
ADD COLUMN location VARCHAR;

UPDATE dim_store_details
SET location = store_type;

UPDATE dim_store_details
SET location = 
    CASE 
        WHEN store_type IN ('Local', 'Mall Kiosk', 'Outlet', 'Super Store') THEN 'Offline'
		WHEN store_type IN ('Web Portal') THEN 'Web'
        ELSE store_type
    END


--- FILTERING VIA STORE TYPE ---

SELECT 
	COUNT(*) AS numbers_of_sales, 
	SUM(product_quantity) AS product_quantity_count,
CASE
	WHEN store_code LIKE 'WEB%' THEN 'Web'
	ELSE 'Offline'
	END AS location
FROM 
	orders_table
GROUP BY 
	location
ORDER BY 
	location DESC;

--- PERCENTAGES ---

WITH sales_data AS (
    SELECT 
		orders_table.product_quantity,
        orders_table.product_quantity * dim_products.product_price AS sales,
		dim_store_details.store_type
    FROM 
        orders_table
    JOIN 
        dim_products ON orders_table.product_code = dim_products.product_code
    JOIN 
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
	FULL JOIN 
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
)

SELECT 
	total_sales,
	total_sales / SUM(total_sales) OVER () * 100 AS percentage_total,
	store_type
FROM (
	SELECT
		SUM(sales) AS total_sales, 
		store_type
	FROM
		sales_data
	GROUP BY 
		store_type
)
ORDER BY 
	total_sales DESC;

--- DOUBLE GROUP BY ---

WITH sales_data AS (
    SELECT 
		orders_table.product_quantity,
        orders_table.product_quantity * dim_products.product_price AS sales,
		dim_date_times.month,
		dim_date_times.year
    FROM 
        orders_table
    JOIN 
        dim_products ON orders_table.product_code = dim_products.product_code
    JOIN 
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
)

SELECT
	SUM(sales) AS total_sales,
	year,
	month
FROM
	sales_data
GROUP BY 
	year, 
	month
ORDER BY
	total_sales DESC
;

--- BASIC AGG & GROUP BY ---

SELECT
	SUM(staff_numbers) AS total_staff_numbers,
	country_code
FROM 
	dim_store_details
GROUP BY
	country_code
ORDER BY 
	total_staff_numbers DESC;

--- JOIN & AGG & WHERE ---

WITH sales_data AS (
    SELECT 
        orders_table.product_quantity * dim_products.product_price AS sales,
		dim_store_details.store_code,
		dim_store_details.store_type,
		dim_store_details.country_code
    FROM 
        orders_table
    JOIN 
        dim_products ON orders_table.product_code = dim_products.product_code
    JOIN 
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
)

SELECT 
	SUM(sales) AS total_sales,
	store_type,
	country_code
FROM
	sales_data
WHERE 
	country_code = 'DE'
GROUP BY 
	store_type, country_code
ORDER BY 
	total_sales;

--- CTE MULTIPLE WITH LEAD --- 

WITH time_data AS (
    SELECT 
		dim_date_times.year,
		CAST(CONCAT(dim_date_times.year, '-', dim_date_times.month, '-', dim_date_times.day, ' ', 
			   dim_date_times.timestamp) AS TIMESTAMP) AS time_combined
	FROM 
        orders_table
    JOIN 
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
)

SELECT
	year,
	AVG(time_diff_between_next_sale) AS actual_time_taken_avg
FROM
(
	SELECT 
		year,
		time_combined,
		next_sale_time,
		next_sale_time - time_combined AS time_diff_between_next_sale
	FROM	
		(	
		SELECT 
			year,
			time_combined,
			LEAD(time_combined) OVER (ORDER BY time_combined) AS next_sale_time

		FROM
			time_data
		)
	)
GROUP BY 
	year
ORDER BY 
	actual_time_taken_avg DESC
;