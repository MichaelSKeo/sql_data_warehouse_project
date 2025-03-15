/*
=========================================================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=========================================================================================================================================
Script Purpose: 
  This stored procedure loads data into the 'bronze' schema from external CSV files. 
  It performs the following actions: 
  - Truncates the bronze tables before loading data.
  - Users the `Bulk Insert` command to load data from csv files into bronze tabels. 

Parameters: 
  None.
  This stored procedure does not accept any parameter or return any values. 

Usage example: 
  EXEC bronze.load_bronze;
=========================================================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	BEGIN TRY -- Add Try & Catch for Error Handling, data integrity, and issue logging for debugging. 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; -- Declare start and end time variables to caculate loading times for each section.  

		SET @batch_start_time = GETDATE();
		PRINT '================================================================================================================';
		PRINT 'Loading Bronze Layer'; 
		PRINT '================================================================================================================';

		PRINT '----------------------------------------------------------------------------------------------------------------'; 
		PRINT 'Loading CRM Data'  
		PRINT '----------------------------------------------------------------------------------------------------------------'; 

		SET @start_time = GETDATE(); -- set the start_time to the current date time when it happens. 
		PRINT '>> Truncating date in table: bronze.crm_cust_info';  
		TRUNCATE TABLE bronze.crm_cust_info; -- Method: FULL LOAD (TRUNCATE AND BULK INSERT) to avoid dupilcates
	
		PRINT '>> Inserting data into bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\Micha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- first row of data start from line 2 
			FIELDTERMINATOR = ',', -- comma separated file format 
			TABLOCK -- lock the whole table

		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '----------------------------';

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating date in table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; 
	
		PRINT '>> Inserting data into bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Micha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK

		) ; 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating date in table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details; 

		PRINT '>> Inserting data into ronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Micha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK

		) ; 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds'; 
		PRINT '----------------------------';

		PRINT '----------------------------------------------------------------------------------------------------------------'; 
		PRINT 'Loading ERP Data';  
		PRINT '----------------------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating date in table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12; 

		PRINT '>> Inserting data into bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Micha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK

		) ; 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating date in table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101; 

		PRINT '>> Inserting data into bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Micha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
	
		) ; 
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '----------------------------';

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating date in table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		PRINT '>> Inserting data into bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Micha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK

		) ; 
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '----------------------------';

		SET @batch_end_time = GETDATE(); 
		PRINT '=============================================';
		PRINT '>> Load Bronze Layer is completed';
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds'; 
		PRINT '=============================================';

	END TRY 
	BEGIN CATCH -- Add catch to show error logs
	PRINT '------------------------------------------------------------------------------------------------------------------------';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'; 
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR MESAAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR); 
	PRINT '------------------------------------------------------------------------------------------------------------------------';
	END CATCH

END
