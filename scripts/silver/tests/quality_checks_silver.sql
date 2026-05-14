
-- =============================================================
-- Data Quality Checks on Silver Layer Tables
-- =============================================================

-- =============================================================
-- silver.crm_cust_info Data Quality Checks
-- =============================================================

-- Check For Nulls or Duplicates in Primay Key Columns in Silver Tables
-- Expectation: No Result
SELECT * FROM silver.crm_cust_info;

SELECT cst_id, 
COUNT(*) 
FROM bronze.crm_cust_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check For Unwanted Spaces in String Columns in Bronze Tables
-- Expectation: No Result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Check the Consistency of values in low cardinality columns in Bronze Tables
-- Quality Check for cst_marital_status and cst_gndr column in silver.crm_cust_info
-- Data Standardization and Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- =============================================================
-- silver.crm_prd_info Data Quality Checks
-- =============================================================
SELECT * FROM silver.crm_prd_info;
-- Check For Nulls or Duplicates in Primay Key Columns in Silver Tables - Products Information
-- Expectation: No Result
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT
prd_key,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_key
HAVING COUNT(*) > 1 OR prd_key IS NULL;

-- Check For Unwanted Spaces in String Columns in Silver - Products Information
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check if we have Null or Negative Prices in the prd_cost column in Silver Tables - Products Information
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Data Standardization and Consistency Check for prd_line column in Silver Tables - Products Information
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders in prd_start_dt and prd_end_dt columns in Silver Tables - Products Information
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- =============================================================
-- silver.crm_sales_details Data Quality Checks
-- =============================================================

SELECT * FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Check for Invalid Dates
SELECT sls_order_dt FROM silver.crm_sales_details
SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check for when Date is less than or equal 0
SELECT sls_order_dt 
FROM silver.crm_sales_details 
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20150101;

-- Check where length of date is not up tp 8
SELECT sls_order_dt FROM silver.crm_sales_details
WHERE LEN(sls_order_dt) != 8;

-- Check the Date Boundary
SELECT sls_order_dt FROM silver.crm_sales_details
WHERE sls_order_dt > 20150101;

-- Check that the order date is older than the shipping date (Invalid Order)
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
OR sls_order_dt > sls_due_dt;

-- Check that the sales is correct
-- Sales = Quatity * Price
-- Non Negative, Non Null and Non Zero
SELECT DISTINCT
sls_sales AS old_sls_sales,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * sls_price 
		THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/NULLIF(sls_quantity,0)
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price;


-- =============================================================
-- silver.erp_loc_a101 Data Quality Checks
-- =============================================================
SELECT 
	cid,
	cntry
FROM silver.erp_loc_a101;

-- Checking for unmatching 'cid' values between the 'silver.erp_loc_a101' and 'silver.crm_cust_info' tables.
SELECT 
	REPLACE(cid,'-','') cid,
	cntry
FROM silver.erp_loc_a101 WHERE REPLACE(cid,'-','') NOT IN
(SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Cheching the Country Data Standardization and Consistency
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- =============================================================
-- silver.erp_cust_az12 Data Quality Checks
-- =============================================================

SELECT *
FROM silver.erp_cust_az12;

SELECT
	cid,
	bdate,
	gen
FROM silver.erp_cust_az12
WHERE cid LIKE '%AW00011000%';

-- To check unmatching data between bronze and silver
SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid, 
	bdate,
	gen
FROM silver.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
		 ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Check for Out of Range Birthdate
SELECT DISTINCT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR  bdate > GETDATE()

-- Data Standardization and Consistency Check For Gender
SELECT DISTINCT
	gen
FROM silver.erp_cust_az12

SELECT DISTINCT
	gen
FROM silver.erp_cust_az12

-- =============================================================
-- silver.erp_px_cat_g1v2 Data Quality Checks
-- =============================================================

SELECT
	id,
	cat,
	subcat,
	maintenance
FROM silver.erp_px_cat_g1v2;

-- Check for unwanted spaces
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
OR subcat != TRIM(subcat)
OR maintenance != TRIM(maintenance);

-- Check the Data Standardization and Consistency
SELECT DISTINCT
	cat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT
	subcat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT
	maintenance
FROM silver.erp_px_cat_g1v2;
