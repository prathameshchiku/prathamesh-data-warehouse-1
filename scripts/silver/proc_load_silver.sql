/*
==============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==============================================================================
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
  EXEC silver.load_silver;
==============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT'============================================';
		PRINT'           LOADING SILVER LAYER             ';
		PRINT'============================================';
		PRINT'--------------------------------------------';
		PRINT'            Loading CRM Tables              ';
		PRINT'--------------------------------------------';
		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS
		cst_marital_status,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS
		cst_gndr,
		cst_create_date
		FROM(
		SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		)t WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT'>>-----------------------------------------<<';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)

		select 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
			SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE Upper(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'R' THEN 'Road'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			prd_start_dt,
			DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		from bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT'>>-----------------------------------------<<';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)

		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales is null or sls_sales != sls_quantity*sls_price THEN sls_quantity*ABS(sls_price)
			 ELSE sls_sales
		end as
		sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL THEN sls_sales/sls_quantity
			 ELSE ABS(sls_price)
			 end as
		sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';

		PRINT'--------------------------------------------';
		PRINT'            Loading ERP Tables              ';
		PRINT'--------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_CUST_AZ12'
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		PRINT '>> Inserting Data Into: silver.erp_CUST_AZ12'
		INSERT INTO silver.erp_CUST_AZ12 (
		CID,
		BDATE,
		GEN
		)
		select
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
			ELSE CID
			END AS
		CID,
		CASE WHEN BDATE > GETDATE() THEN NULL
		ELSE BDATE
		END AS 
		BDATE,
			CASE WHEN TRIM(GEN) = 'M' THEN 'Male'
				 WHEN TRIM(GEN) = 'F' THEN 'Female'
				 WHEN TRIM(GEN) = '' OR GEN IS NULL THEN 'n/a'
				 ELSE TRIM(GEN)
			END as 
		GEN
		from 
		bronze.erp_CUST_AZ12;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT'>>-----------------------------------------<<';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_LOC_A101'
		TRUNCATE TABLE silver.erp_LOC_A101;
		PRINT '>> Inserting Data Into: silver.erp_LOC_A101'
		INSERT INTO silver.erp_LOC_A101(
		CID,
		CNTRY
		)
		SELECT
		REPLACE(CID, '-', '') CID,
		CASE WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(CNTRY) IS NULL OR CNTRY = '' THEN 'n/a'
			 WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			 ELSE TRIM(CNTRY)
		END AS
		CNTRY
		FROM
		bronze.erp_LOC_A101;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT'>>-----------------------------------------<<';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_PX_CAT_G1V2'
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Data Into: silver.erp_PX_CAT_G1V2'
		INSERT INTO silver.erp_PX_CAT_G1V2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		)
		select 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		FROM
		bronze.erp_PX_CAT_G1V2;
		SET @end_time = GETDATE();
		SET @batch_end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT'>>=========================================<<';
		PRINT'LOADING BRONZE LAYER IS COMPLETED';
		PRINT'>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time,@batch_end_time) AS VARCHAR) + ' seconds';
		PRINT'>>=========================================<<';
	END TRY
	BEGIN CATCH
		PRINT'============================================';
		PRINT'AN ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Number' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT'Error Number' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT'============================================';
	END CATCH
END 

EXEC silver.load_silver;
