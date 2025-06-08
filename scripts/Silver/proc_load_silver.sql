/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
	LANGUAGE plpgsql
	AS $$
	DECLARE 
	BEGIN
		RAISE NOTICE '=================================================================';
		RAISE NOTICE 'Loading Silver Layer';
		RAISE NOTICE '=================================================================';
		
		
		
		RAISE NOTICE '-----------------------------------------------------------------';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '-----------------------------------------------------------------';
	
		RAISE NOTICE '>> Truncating Table : silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		
		RAISE NOTICE '>> Inserting Data Into : silver.crm_cust_info';
				INSERT INTO silver.crm_cust_info 
			(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_martial_status,
				cst_gndr,
				cst_create_date
			)
			SELECT 
				cst_id,
				cst_key,
				TRIM(cst_firstname) As cst_firstname,
				TRIM(cst_lastname) AS cst_lastname,
				CASE WHEN UPPER(TRIM(cst_martial_status))= 'M' THEN 'MARRIED'
				 	 WHEN UPPER(TRIM(cst_martial_status))='S' THEN 'SINGLE'
				 	 ELSE 'n/a'
				END cst_martial_status,
				CASE WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'MALE'
				 	 WHEN UPPER(TRIM(cst_gndr))='F' THEN 'FEMALE'
				 	 ELSE 'n/a'
				END cst_gndr,
				cst_create_date
			FROM
			(
			SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as Flag_last
			FROM bronze.crm_cust_info
			) WHERE Flag_last = 1;
	
		RAISE NOTICE '>> Truncating Table : silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
	
		RAISE NOTICE '>> Inserting Data Into : silver.crm_prd_info';
			INSERT INTO silver.crm_prd_info(
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
						REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
						SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,        -- Extract product key
						prd_nm,
						CAST(COALESCE(prd_cost,'0') AS INT) AS prd_cost,
						CASE 
							WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
							WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
							WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
							WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
							ELSE 'n/a'
						END AS prd_line, -- Map product line codes to descriptive values
						prd_start_dt,
						LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
						AS prd_end_dt -- Calculate end date as one day before the next start date
				FROM bronze.crm_prd_info;
	
		RAISE NOTICE '>> Truncating Table : silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
	
		RAISE NOTICE '>> Inserting Data Into : silver.crm_sales_details';
					
				INSERT INTO silver.crm_sales_details (
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_ord_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE 
					WHEN sls_ord_dt = 0 OR LENGTH(CAST(sls_ord_dt AS TEXT)) != 8 THEN NULL
					ELSE CAST(CAST(sls_ord_dt AS TEXT) AS DATE)
				END AS sls_ord_dt,
				CASE 
					WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS TEXT) AS DATE)
				END AS sls_ship_dt,
				CASE 
					WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT))!= 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS TEXT) AS DATE)
				END AS sls_due_dt,
				CASE 
					WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
						THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
				sls_quantity,
				CASE 
					WHEN sls_price IS NULL OR sls_price <= 0 
						THEN sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price  -- Derive price if original value is invalid
				END AS sls_price
			FROM bronze.crm_sales_details;
	
		RAISE NOTICE '-----------------------------------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '-----------------------------------------------------------------';
	
		
		RAISE NOTICE '>> Truncating Table : silver.erp_CUST_AZ12';
		TRUNCATE TABLE silver.erp_CUST_AZ12;
	
		RAISE NOTICE '>> Inserting Data Into : silver.erp_CUST_AZ12';
					INSERT INTO silver.erp_cust_az12 
					(
						cid,
						bdate,
						gen
					)
				SELECT
						CASE
							WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix if present
							ELSE cid
						END AS cid, 
						CASE
							WHEN bdate > NOW() THEN NULL
							ELSE bdate
						END AS bdate, -- Set future birthdates to NULL
						CASE
							WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
							WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
							ELSE 'n/a'
						END AS gen -- Normalize gender values and handle unknown cases
				FROM bronze.erp_cust_az12;
			
		RAISE NOTICE '>> Truncating Table : silver.erp_LOC_A101';
		TRUNCATE TABLE silver.erp_LOC_A101;
		RAISE NOTICE '>> Inserting Data Into : silver.erp_LOC_A101';
					
					INSERT INTO silver.erp_loc_a101 (
						cid,
						cntry
					)
				SELECT
						REPLACE(cid, '-', '') AS cid, 
						CASE
							WHEN TRIM(cntry) = 'DE' THEN 'Germany'
							WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
							WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
							ELSE TRIM(cntry)
						END AS cntry -- Normalize and Handle missing or blank country codes
				FROM bronze.erp_loc_a101;
			
		RAISE NOTICE '>> Truncating Table : silver.erp_PX_CAT_G1V2';
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		RAISE NOTICE '>> Inserting Data Into : silver.erp_PX_CAT_G1V2';
				INSERT INTO silver.erp_px_cat_g1v2 (
						id,
						cat,
						subcat,
						maintenance
					)
				SELECT
						id,
						cat,
						subcat,
						maintenance
				FROM bronze.erp_px_cat_g1v2;
	END;
	$$;
