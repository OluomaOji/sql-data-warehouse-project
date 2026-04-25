/*
===============================================
Stored Procedure: bronze.load_bronze (Source -> Bronze Layer)
================================================
Script Purpose: This stored procedure loads data from source CSV files into the bronze layer tables in the DataWarehouse database.

How to Use: Execute the stored procedure using the following command:
			EXEC bronze.load_bronze;
===============================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================';
		PRINT 'Loading Bronze Layer..';
		PRINT '================================';

		PRINT '--------------------------------';
		PRINT 'Loading CRM Tables..';
		PRINT '--------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '<< Truncating and Loading bronze.crm_cust_info >>';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\RoboGenesis\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '<< Truncating and Loading bronze.crm_prd_info >>';
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\RoboGenesis\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '<< Truncating and Loading bronze.crm_sales_details >>';
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\RoboGenesis\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
		PRINT '--------------------------------';

		PRINT '--------------------------------';
		PRINT 'Loading ERP Tables..';
		PRINT '--------------------------------';
		PRINT '<< Truncating and Loading bronze.erp_cust_az12 >>';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\RoboGenesis\Documents\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '<< Truncating and Loading bronze.erp_loc_a101 >>';
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\RoboGenesis\Documents\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '<< Truncating and Loading bronze.erp_px_cat_g1v2 >>';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\RoboGenesis\Documents\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
		PRINT '--------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '================================';
		Print 'Bronze Layer Load Completed Successfully!';
		Print 'Total Time taken to load Bronze Layer: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR(10)) + ' seconds';
		PRINT '================================';

	END TRY
	BEGIN CATCH
		PRINT 'Error occurred while loading Bronze Layer: ' + ERROR_MESSAGE();
	END CATCH;
END;



