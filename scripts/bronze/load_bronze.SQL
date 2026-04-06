CREATE OR ALTER PROCEDURE load_bronze
	AS BEGIN

		BEGIN TRY
		--bringing crm tables now

		PRINT'========================'
		PRINT 'starting bronze layer';
		PRINT'========================'

		DECLARE @start_time DATE,@end_time DATE;
		DECLARE @total_time_start DATE,@total_time_end DATE;


		SET @total_time_start=GETDATE();

		SET @start_time=GETDATE();

		PRINT '>>insertion in crm_sales_details starting'

		TRUNCATE TABLE bronze.crm_sales_details;

		BULK  INSERT  bronze.crm_sales_details FROM 
		'E:\work zone\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);

		PRINT '>>insertion in crm_sales_details ending'
		SET @end_time=GETDATE();

		PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));

		PRINT '>>insertion in crm_cust_info starting';

		SET @start_time=GETDATE();

		TRUNCATE TABLE bronze.crm_cust_info;

		BULK INSERT bronze.crm_cust_info FROM 
		'E:\work zone\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);

		PRINT '>>insertion in crm_cust_info ending'
		SET @end_time=GETDATE();

		PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));


		PRINT '>>insertion in crm_prd_info starting';


		SET @start_time=GETDATE();

		TRUNCATE TABLE bronze.crm_prd_info;


		BULK INSERT bronze.crm_prd_info FROM 
		'E:\work zone\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();

		PRINT '>>insertion in crm_prd_info ending'
		PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));


		--bringing erp tables now

		PRINT '>>insertion in erp_px_Cat_g1v2 starting'


		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		SET @start_time=GETDATE();

		BULK INSERT bronze.erp_px_cat_g1v2 FROM
		'E:\work zone\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();

		PRINT '>>insertion in erp_px_Cat_g1v2 ending'
		PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));


		PRINT '>>insertion in erp_loc_a101 starting'
		SET @start_time=GETDATE();


		TRUNCATE TABLE bronze.erp_loc_a101;

		BULK INSERT bronze.erp_loc_a101 FROM
		'E:\work zone\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();

		PRINT '>>insertion in erp_loc_a101 ending'

		PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));


		PRINT '>>insertion in erp_cust_az12 starting'


		TRUNCATE TABLE bronze.erp_cust_az12;

		SET @start_time=GETDATE();

		BULK INSERT bronze.erp_cust_az12 FROM
		'E:\work zone\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);

		 SET @end_time=GETDATE();


		PRINT '>>insertion in erp_cust_az12 ending'

		PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));


		PRINT'========================'
		PRINT 'ending bronze layer'
		PRINT'========================';
		SET @total_time_end=GETDATE();

		PRINT 'Total time taken to load the entire bronze layer:'+CAST(DATEDIFF(SECOND,@total_time_start,@total_time_end)
		as VARCHAR(10));

		END TRY

		BEGIN CATCH

		PRINT 'Loading failed cos some error ocurred'
		PRINT 'Error is '+(ERROR_MESSAGE())
		PRINT 'Error no is '+CAST(ERROR_NUMBER() AS VARCHAR(10))
		PRINT 'Error line no is '+CAST(ERROR_LINE() AS VARCHAR(10))

		END CATCH

END

