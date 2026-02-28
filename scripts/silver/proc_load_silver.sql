--- cleaning and inserting data into table silver.crm_cust_info
INSERT INTO silver.crm_cust_info(
	cst_id, 
	cst_key, 
	cst_firstname, 
	cst_lastname,
	cst_gndr, 
	cst_martial_status,
	cst_create_date
)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE 
	WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
	WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
	ELSE 'n/a'
	END AS cst_gndr,
CASE
	WHEN UPPER(cst_martial_status) = 'M' THEN 'Married'
	WHEN UPPER(cst_martial_status) = 'S' THEN 'Single'
	ELSE 'n/a'
	END AS cst_martial_status,
cst_create_date
FROM(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) AS t 
WHERE flag_last = 1 

--- cleaning and inserting data into silver.crm_prd_info
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
prd_nm,
COALESCE(prd_cost, 0) AS prd_cost,
CASE UPPER(prd_line)
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Sport'
	WHEN 'M' THEN 'Mountain'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a' 
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_date,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

--- cleaning and inserting data into silver.crm_sales_details
INSERT INTO silver.crm_sales_details (
	sls_ord_num  ,
    sls_prd_key  ,
    sls_cust_id  ,
    sls_order_dt ,
    sls_ship_dt  ,
    sls_due_dt   ,
    sls_sales    ,
    sls_quantity ,
    sls_price
)
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
	WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 
	THEN NULL
	ELSE CONVERT(DATE,CAST(sls_order_dt AS VARCHAR(8)), 112)
	END AS sls_order_dt,
CASE 
	WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 
	THEN NULL
	ELSE CONVERT(DATE,CAST(sls_ship_dt AS VARCHAR(8)), 112)
	END AS sls_ship_dt,
CASE 
	WHEN LEN(sls_due_dt) != 8 OR sls_due_dt = 0 
	THEN NULL
	ELSE CONVERT(DATE,CAST(sls_due_dt AS VARCHAR(8)), 112)
	END AS sls_due_dt,
CASE WHEN sls_price IS NULL
	 THEN sls_sales / NULLIF(sls_quantity,0)
	 ELSE ABS(sls_price)
	 END AS sls_price,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_quantity IS NULL
	 THEN sls_sales
	 ELSE ABS(sls_price) * sls_quantity
	 END AS sls_sales
FROM bronze.crm_sales_details
