CREATE OR ALTER PROCEDURE load_silver

AS BEGIN

    BEGIN TRY

        PRINT'------------------'
        PRINT'--Starting SILVER LAYER-CRM--'
        PRINT'------------------'

        DECLARE @start_time DATE,@end_time DATE;
        DECLARE @total_time_start DATE,@total_time_end DATE;

        SET @total_time_start=GETDATE();

        SET @start_time=GETDATE();

        PRINT'>>insertion in crm_cust_info starting'
        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,
        cst_gndr,cst_create_date
        )

        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE UPPER(TRIM(cst_marital_status)) 
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE 'n/a'  
            END
            AS cst_marital_status,
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info where cst_id IS NOT NULL
        ) AS t
        WHERE flag_last = 1;

        SET @end_time=GETDATE();

        PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));

        PRINT'>>insertion in crm_cust_info ending'

        --WE FIRST OF REMOVED DATA WHERE CST_ID WAS APPEARING REPEATEDLY THEN WE DID DATA NORMALIZATION AND STANDARDIZATION
        --BY REPLACING GENDER AND MARITAL WITH CORRESPONDING AND LEAVING NULL AS 'N/A' AND THEN WE JUST INSERT ALL 
        --OF THEM INTO SILVER TABLE

        PRINT'>>insertion in crm_sales_details starting'

        SET @start_time=GETDATE();


        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,
        sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)

        SELECT  sls_ord_num,sls_prd_key
        ,sls_cust_id,

        CASE 
        WHEN sls_order_dt=0 THEN NULL
        WHEN LEN(sls_order_dt)!=8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE) 
        END AS sls_order_dt
        ,CASE 
        WHEN sls_ship_dt=0 THEN NULL
        WHEN LEN(sls_ship_dt)!=8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE) 
        END AS sls_ship_dt,
        CASE 
        WHEN sls_due_dt=0 THEN NULL
        WHEN LEN(sls_due_dt)!=8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE) 
        END AS sls_due_dt
        ,CASE WHEN sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*ABS(sls_price) 
        THEN  sls_quantity*ABS(sls_price)
        ELSE sls_sales

        END AS sls_sales
        ,sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price<=0 
        THEN sls_sales/NULLIF(sls_quantity,0)
        ELSE sls_price
        END AS sls_price


        FROM bronze.crm_sales_details

        SET @end_time=GETDATE();
        PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));

        PRINT'>>insertion in crm_sales_details ending'



        PRINT'>>insertion in crm_prd_info starting'
        SET @start_time=GETDATE();

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)

        SELECT prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt FROM
        (SELECT prd_id,REPLACE(SUBSTRING(prd_key,1,5),'-','_')AS cat_id
        ,SUBSTRING(prd_key,7,LEN(prd_key))as prd_key,prd_nm,COALESCE(prd_cost,0)AS prd_cost,
        CASE UPPER(TRIM(prd_line)) 
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
        END AS prd_line
        ,CAST(prd_start_dt AS DATE)as prd_start_dt,
        CAST(LEAD(prd_start_dt)over(partition by prd_key order by prd_start_dt)AS DATE)as prd_end_dt
        FROM bronze.crm_prd_info)t


        --okay so we need to replace end date with the start date of next one

        SET @end_time=GETDATE();
        PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));

        PRINT'>>insertion in crm_prd_info ending'


        PRINT'--STARTING SILVER LAYER-ERP--'




        PRINT'>>insertion in erp_cust_az_12 starting'
        SET @start_time=GETDATE();


        TRUNCATE TABLE silver.erp_cust_az12 ;

        INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)

        SELECT cid,bdate,gen FROM(
        SELECT CASE 
        WHEN cid LIKE 'NAS%' THEN UPPER(SUBSTRING(cid,4,LEN(cid)))
        ELSE UPPER(cid) 

        END AS cid,
        CASE WHEN bdate>GETDATE() THEN NULL
        ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
            WHEN TRIM(gen) = '' OR gen IS NULL THEN 'n/a'
            ELSE gen
        END AS gen

        FROM bronze.erp_cust_az12  
        )t



        SET @end_time=GETDATE();
        PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));

        PRINT'>>insertion in erp_cust_az_12 ending'



        PRINT'>>insertion in erp_loc_a101 starting'
        SET @start_time=GETDATE();

        TRUNCATE TABLE silver.erp_loc_a101

        INSERT INTO silver.erp_loc_a101(cid,cntry)

        SELECT REPLACE(cid,'-','')as cid,
        CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
        WHEN TRIM(cntry)IN ('UK','GB') THEN 'United Kingdom'
        WHEN cntry IS NULL THEN 'n/a'
        WHEN cntry='' THEN 'n/a'
        ELSE TRIM(cntry)
        END AS cntry

        FROM bronze.erp_loc_a101 



        SET @end_time=GETDATE();

        PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));

        PRINT'>>insertion in erp_loc_a101 ending'


        PRINT'>>insertion in erp_pc_cat_g1v2 starting'
        SET @start_time=GETDATE();

        TRUNCATE TABLE silver.erp_px_cat_g1v2

        INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)

        SELECT id,cat,subcat,maintenance FROM bronze.erp_px_cat_g1v2 




        SET @end_time=GETDATE();

        PRINT'Total time for loading in seconds is '+CAST(DATEDIFF(SECOND,@start_time,@end_time)as VARCHAR(10));

        PRINT'>>insertion in erp_px_cat_g1v2 ending'





        SET @total_time_end=GETDATE();

        PRINT 'Total time taken to load the entire silver
        layer:'+CAST(DATEDIFF(SECOND,@total_time_start,@total_time_end)
		        as VARCHAR(10));


        PRINT'------------------'
        PRINT'--ENDING SILVER LAYER--'
        PRINT'------------------'


        END TRY



    BEGIN CATCH


		    PRINT 'Loading silver layer failed cos some error ocurred'
		    PRINT 'Error is '+(ERROR_MESSAGE())
		    PRINT 'Error no is '+CAST(ERROR_NUMBER() AS VARCHAR(10))
		    PRINT 'Error line no is '+CAST(ERROR_LINE() AS VARCHAR(10))


    END CATCH

END
