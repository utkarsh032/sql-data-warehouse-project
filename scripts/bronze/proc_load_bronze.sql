
--=========================================================
-- BRONZE LAYER BUILD STORED PROCEDURE
--=========================================================

/*
Naming Convention
-----------------
All stored procedures responsible for loading data must
follow the naming pattern:

    load_<layer>

Where:
    <layer> represents the data warehouse layer.

Examples:
    load_bronze  -> Loads data into Bronze layer
    load_silver  -> Loads data into Silver layer
    load_gold    -> Loads data into Gold layer
*/


/*
============================================================
Stored Procedure: bronze.load_bronze
============================================================

Purpose
-------
Loads raw CSV files from CRM and ERP source systems into
Bronze layer tables using BULK INSERT.

Process
-------
1. Truncate existing Bronze tables
2. Load fresh data from CSV files
3. Track execution time for each table
4. Print progress logs
5. Handle errors using TRY/CATCH

Note
----
Bronze layer stores raw ingested data with minimal
transformation.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

    DECLARE @start_time DATETIME, @end_time   DATETIME,  @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE()

        PRINT '=========================================================';
        PRINT 'STARTING BRONZE LAYER DATA LOAD';
        PRINT 'Execution Time: ' + CAST(GETDATE() AS NVARCHAR);
        PRINT '=========================================================';


        ----------------------------------------------------
        -- CRM TABLES
        ----------------------------------------------------

        PRINT 'Loading CRM Tables...';


        ----------------------------------------------------
        -- CRM Customer Info
        ----------------------------------------------------
        PRINT '>> Loading bronze.crm_cust_info';

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'D:\Utkarsh\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '✔ crm_cust_info loaded successfully.';
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds.';
        PRINT '---------------------------------------------------------';


        ----------------------------------------------------
        -- CRM Product Info
        ----------------------------------------------------
        PRINT '>> Loading bronze.crm_prd_info';

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Utkarsh\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '✔ crm_prd_info loaded successfully.';
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds.';
        PRINT '---------------------------------------------------------';


        ----------------------------------------------------
        -- CRM Sales Details
        ----------------------------------------------------
        PRINT '>> Loading bronze.crm_sales_details';

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'D:\Utkarsh\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '✔ crm_sales_details loaded successfully.';
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds.';
        PRINT '---------------------------------------------------------';



        ----------------------------------------------------
        -- ERP TABLES
        ----------------------------------------------------

        PRINT 'Loading ERP Tables...';


        ----------------------------------------------------
        -- ERP Customer Data
        ----------------------------------------------------
        PRINT '>> Loading bronze.erp_cust_az12';

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\Utkarsh\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '✔ erp_cust_az12 loaded successfully.';
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds.';
        PRINT '---------------------------------------------------------';



        ----------------------------------------------------
        -- ERP Location Data
        ----------------------------------------------------
        PRINT '>> Loading bronze.erp_loc_a101';

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\Utkarsh\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '✔ erp_loc_a101 loaded successfully.';
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds.';
        PRINT '---------------------------------------------------------';



        ----------------------------------------------------
        -- ERP Product Category
        ----------------------------------------------------
        PRINT '>> Loading bronze.erp_px_cat_g1v2';

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\Utkarsh\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '✔ erp_px_cat_g1v2 loaded successfully.';
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds.';
        SET @batch_end_time = GETDATE()


        PRINT '=========================================================';
        PRINT 'Total Loading Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(20)) + ' seconds.';
        PRINT 'BRONZE LAYER LOAD COMPLETED SUCCESSFULLY';
        PRINT '=========================================================';

    END TRY

    BEGIN CATCH

        PRINT '=========================================================';
        PRINT 'ERROR OCCURRED WHILE LOADING BRONZE LAYER';
        PRINT '=========================================================';

        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR(50));
        PRINT 'Error Line    : ' + CAST(ERROR_LINE() AS NVARCHAR(50));

    END CATCH

END;
GO

EXEC bronze.load_bronze;
