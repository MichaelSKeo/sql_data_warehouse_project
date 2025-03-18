CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	BEGIN TRY
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	
		SET @batch_start_time = GETDATE();
		PRINT '================================================================================================================';
		PRINT 'Loading Silver Layer'; 
		PRINT '================================================================================================================';

		PRINT '----------------------------------------------------------------------------------------------------------------'; 
		PRINT 'Loading CRM Data'  
		PRINT '----------------------------------------------------------------------------------------------------------------'; 
	
		SET @start_time = GETDATE(); 
		TRUNCATE TABLE silver.crm_cust_info; 
		PRINT '>> Inserting data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
	
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname, 
			cst_marital_status,
			cst_gndr, 
			cst_create_date 
		) 

		select 
			cst_id, 
			trim(cst_key) as cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname, 
			case 
				when upper(trim(cst_marital_status)) = 'S' then 'Single' 
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				else 'Unknown' end as cst_marital_status, 
			case 
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				when upper(trim(cst_gndr)) = 'F' then 'Female'
				else 'Unknown' end as cst_gndr, 
			cst_create_date


		from (
			select *
				, row_number() over (partition by cst_id order by cst_create_date desc) as rn 
			from bronze.crm_cust_info
			where cst_id is not null 

		) a 

		where rn = 1 ; 
		
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second'; 
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncate table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info; 
		PRINT '>> Inserting data into: silver.crm_prd_info';
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

		select
			prd_id,
			-- prd_key,
			trim(replace(substring(prd_key, 1, 5), '-', '_')) as cat_id, 
			trim(substring(prd_key, 7, len(prd_key))) as prd_key,  
			prd_nm, 
			isnull(prd_cost, 0)as prd_cost, 
			case upper(trim(prd_line))
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				else 'Unknown' 
			end as prd_line,
			cast(prd_start_dt as date) as prd_start_dt, 
			cast(dateadd(day, -1, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as date) as prd_end_dt  

		from bronze.crm_prd_info; 

		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------';

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data into: silver.crm_sales_details'; 
		INSERT INTO silver.crm_sales_details (

			sls_ord_num, 
			sls_prd_key, 
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt, 
			sls_sales, 
			sls_quantity, 
			sls_price

		)

		select 
			sls_ord_num, 
			sls_prd_key, 
			sls_cust_id,
			cast(cast(case 
				when sls_order_dt <= 0 or len(sls_order_dt) != 8 then null 
				else sls_order_dt end 
			as VARCHAR) as date) as sls_order_dt, 
			cast(cast(sls_ship_dt as VARCHAR) as date) as sls_ship_dt, 
			cast(cast(sls_due_dt as VARCHAR) as date) as sls_due_dt, 

			case 
				when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price) then sls_quantity * ABS(sls_price) 
				else sls_sales end as sls_sales, 

			sls_quantity, 
			case 
				when sls_price is null or sls_price <= 0
				then sls_sales / nullif(sls_quantity, 0)
				else sls_price
			end as sls_price

		from bronze.crm_sales_details

		PRINT '>> Trucating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12; 
		PRINT '>> Inserting data into: silver.erp_cust_az12'; 
		INSERT INTO silver.erp_cust_az12 (
			cid, 
			bdate, 
			gen

		) 

		select 
			case when cid like 'NAS%' then substring(trim(cid), 4, len(trim(cid)))
				else trim(cid) end as cid, 
			case when bdate > GETDATE() then null else bdate end as bdate, 
			case  
				when upper(trim(gen)) is null or upper(trim(gen)) = ''  then 'Unknown'
				when upper(trim(gen)) = 'M' then 'Male'
				when upper(trim(gen)) = 'F' then 'Female' 
				else trim(gen) 
			end as gen

		from bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second'; 


		PRINT '----------------------------------------------------------------------------------------------------------------'; 
		PRINT 'Loading ERP Data'  
		PRINT '----------------------------------------------------------------------------------------------------------------'; 

		SET @start_time = GETDATE()
		PRINT '>> Trucating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101; 
		PRINT '>> Inserting data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (

			cid, 
			cntry

		) 

		select 

			replace(trim(cid),'-', '') as cid, 
			case 
				when upper(trim(cntry)) is null or upper(trim(cntry)) = '' then 'Unknown'
				when upper(trim(cntry)) in ('USA', 'US') then 'United States' 
				when  upper(trim(cntry)) = 'DE' then 'Germany'
				else trim(cntry)
			end as cntry


		from bronze.erp_loc_a101
	
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 

		SET @start_time = GETDATE();
		PRINT '>> Truncating table silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2; 
		PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (

			id, 
			cat, 
			subcat, 
			maintenance

		) 

		select 
			id,
			cat,
			subcat, 
			maintenance 
		from bronze.erp_px_cat_g1v2 ; 

		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 

		SET @batch_end_time = GETDATE();
		PRINT '=============================================';
		PRINT '>> Load Bronze Layer is completed';
		PRINT '>> Total Load Duration: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '=============================================';

	END TRY
	BEGIN CATCH 
	PRINT '--------------------------------------------------------------------------------------------------------------'
	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR); 
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '--------------------------------------------------------------------------------------------------------------'
	END CATCH

END 
