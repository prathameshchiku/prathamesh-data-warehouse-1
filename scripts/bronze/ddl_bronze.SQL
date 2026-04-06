IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id int,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);

IF OBJECT_ID ('bronze.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12(
	CID VARCHAR(50),	
	BDATE DATE,
	GEN VARCHAR(50)

);

IF OBJECT_ID ('bronze.erp_LOC_A101','U') IS NOT NULL
	DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101(
	CID VARCHAR(50),
	CNTRY VARCHAR(50)
);

IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id int,
	prd_key	VARCHAR(50),
	prd_nm	VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num	VARCHAR(50),
	sls_prd_key	VARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt INT,	
	sls_ship_dt INT,	
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2(
	ID VARCHAR(50),
	CAT VARCHAR(50),	
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(50)
);

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT'============================================';
		PRINT'           LOADING BRONZE LAYER             ';
		PRINT'============================================';
		PRINT'--------------------------------------------';
		PRINT'            Loading CRM Tables              ';
		PRINT'--------------------------------------------';
		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Vader\Desktop\DataWarehouse Project\Dataset\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT'>>-----------------------------------------<<';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Vader\Desktop\DataWarehouse Project\Dataset\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT'>>-----------------------------------------<<';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Vader\Desktop\DataWarehouse Project\Dataset\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';

		PRINT'--------------------------------------------';
		PRINT'            Loading ERP Tables              ';
		PRINT'--------------------------------------------';
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT'>> Inserting Data Into: bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\Vader\Desktop\DataWarehouse Project\Dataset\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT'>>-----------------------------------------<<';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT'>> Inserting Data Into: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\Vader\Desktop\DataWarehouse Project\Dataset\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT'>>-----------------------------------------<<';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT'>> Inserting Data Into: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\Vader\Desktop\DataWarehouse Project\Dataset\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
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
		PRINT'AN ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Number' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT'Error Number' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT'============================================';
	END CATCH
END


EXEC bronze.load_bronze;
