/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze() 
LANGUAGE plpgsql
AS $$
DEClARE
BEGIN
	RAISE NOTICE '=================================================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '=================================================================';
	

	
	RAISE NOTICE '-----------------------------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '-----------------------------------------------------------------';

	RAISE NOTICE '>> Truncating Table : bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	
	RAISE NOTICE '>> Inserting Data Into : bronze.crm_cust_info';
	COPY bronze.crm_cust_info FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' DELIMITER ',' CSV HEADER;
	
	RAISE NOTICE '>> Truncating Table : bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	RAISE NOTICE '>> Inserting Data Into : bronze.crm_prd_info';
	COPY bronze.crm_prd_info FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' DELIMITER ',' CSV HEADER;
	
	RAISE NOTICE '>> Truncating Table : bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	RAISE NOTICE '>> Inserting Data Into : bronze.crm_sales_details';
	COPY bronze.crm_sales_details FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' DELIMITER ',' CSV HEADER;
	
	RAISE NOTICE '-----------------------------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '-----------------------------------------------------------------';

	RAISE NOTICE '>> Truncating Table : bronze.erp_CUST_AZ12';
	TRUNCATE TABLE bronze.erp_CUST_AZ12;

	RAISE NOTICE '>> Inserting Data Into : bronze.erp_CUST_AZ12';
	COPY bronze.erp_CUST_AZ12 FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' DELIMITER ',' CSV HEADER;
	
	RAISE NOTICE '>> Truncating Table : bronze.erp_LOC_A101';
	TRUNCATE TABLE bronze.erp_LOC_A101;

	RAISE NOTICE '>> Inserting Data Into : bronze.erp_LOC_A101';
	COPY bronze.erp_LOC_A101 FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' DELIMITER ',' CSV HEADER;
	
	RAISE NOTICE '>> Truncating Table : bronze.erp_PX_CAT_G1V2';
	TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

	RAISE NOTICE '>> Inserting Data Into : bronze.erp_PX_CAT_G1V2';
	COPY bronze.erp_PX_CAT_G1V2 FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
	
END;
$$;
