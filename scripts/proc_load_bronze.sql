/*
-----------------------------------------------------------
create stored porc = load bronze layer from source (crm+erp)
------------------------------------------------------------------
script purpose:
    this stored proc loads data from source csv files to bronze layer schema tables
    perform two action one is truncate existing table data & bulk insert new data into bronze tabels
    also display time required to complete the process 

for excecution of stored proc : exec bronze.load_bronze
------------------------------------------------------------------
/*

CREATE OR ALTER PROC bronze.load_bronze as 
BEGIN 
	 declare @start_time DATETIME , @end_time DATETIME,@Batch_start_time datetime,@batch_end_time datetime ;
     BEGIN TRY
	        SET @Batch_start_time = GETDATE();
			PRINT '========================';
			PRINT 'Loading bronze layer';
			PRINT '========================';


			PRINT '-------------------------';
			PRINT 'LOADING CRM Tables';
			PRINT '-------------------------';

			SET @start_time = getdate();
			PRINT '>> TRUNCATING TABLE:bronze.crm_cust_info';
			TRUNCATE table bronze.crm_cust_info --IF WE RUN MULTIPLE TIME ITS ENSURE NO DUPLICATES

			PRINT '>> INSERTING DATA INTO:bronze.crm_cust_info';
			BULK INSERT bronze.crm_cust_info
			from 'C:\Users\sanke\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',' ,
					TABLOCK 
			);
			SET @end_time = getdate();
			print 'Load Duration' + CAST (datediff(second,@start_time,@end_time) as nvarchar)+'seconds'

			SET @start_time = getdate();

			PRINT '>> TRUNCATING TABLE:bronze.crm_prd_info';

			TRUNCATE TABLE bronze.crm_prd_info;

			PRINT '>> INSERTING DATA INTO:bronze.crm_prd_info';
			BULK INSERT bronze.crm_prd_info
			from 'C:\Users\sanke\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',' ,
					TABLOCK 
			);
			SET @end_time = getdate();
			print 'Load Duration' + CAST (datediff(second,@start_time,@end_time) as nvarchar)+'seconds'

			SET @start_time = getdate();
			PRINT '>> TRUNCATING TABLE:bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details --IF WE RUN MULTIPLE TIME ITS ENSURE NO DUPLICATES

			PRINT '>> INSERTING DATA INTO:bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_details
			from 'C:\Users\sanke\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',' ,
					TABLOCK 
			);
			SET @end_time = getdate();
			print 'Load Duration' + CAST (datediff(second,@start_time,@end_time) as nvarchar)+'seconds';

			PRINT '-------------------------';
			PRINT 'LOADING ERP Tables';
			PRINT '-------------------------';
			SET @start_time = getdate();
			PRINT '>> TRUNCATING TABLE:bronze.erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12 --IF WE RUN MULTIPLE TIME ITS ENSURE NO DUPLICATES

			PRINT '>> INSERTING DATA INTO:bronze.erp_cust_az12';
			BULK INSERT bronze.erp_cust_az12
			from 'C:\Users\sanke\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',' ,
					TABLOCK 
			);

			SET @end_time = getdate();
			print 'Load Duration' + CAST (datediff(second,@start_time,@end_time) as nvarchar)+'seconds'

			SET @start_time = getdate();
			PRINT '>> TRUNCATING TABLE:bronze.erp_loc_a101';

			TRUNCATE TABLE bronze.erp_loc_a101 --IF WE RUN MULTIPLE TIME ITS ENSURE NO DUPLICATES

			PRINT '>> INSERTING DATA INTO:bronze.erp_loc_a101';
			BULK INSERT bronze.erp_loc_a101
			from 'C:\Users\sanke\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',' ,
					TABLOCK 
			);
			SET @end_time = getdate();
			print 'Load Duration' + CAST (datediff(second,@start_time,@end_time) as nvarchar)+'seconds'

			SET @start_time = getdate();
			PRINT '>> TRUNCATING TABLE:bronze.erp_px_cat_g1v2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2 --IF WE RUN MULTIPLE TIME ITS ENSURE NO DUPLICATES

			PRINT '>> INSERTING DATA INTO:bronze.erp_px_cat_g1v2';
			BULK INSERT bronze.erp_px_cat_g1v2
			from 'C:\Users\sanke\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',' ,
					TABLOCK 
			);
			SET @end_time = getdate();
			print 'Load Duration' + CAST (datediff(second,@start_time,@end_time) as nvarchar)+'seconds'
			SET @batch_end_time = GETDATE();
			PRINT 'Batch duration'+ CAST (datediff(second,@batch_start_time,@batch_end_time) as nvarchar)+'seconds'
	END TRY
		BEGIN CATCH
			PRINT '============================================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'ERROR MEASSAGE' + ERROR_MESSAGE();
			PRINT 'ERROR MEASSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT '============================================================';
		END CATCH
END
    
    
