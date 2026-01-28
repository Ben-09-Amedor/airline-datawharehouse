/*
==================================================================================
Stored Procedure: Load Bronze Layer ( Source -> Bronze)
===================================================================================
Scripts Purpose:
	This procedure loads data into the 'bronze' schema from external csv file.
	This store procedure truncates and load the bronze data. It uses  'BULK INSERT' 
	command to load file into bronze Table

Parameters:
	 None
	This Store procedure does not accept any parameters or return any values

Usage Example:
		EXEC bronze.load_bronze;
===================================================================================

*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time =  GETDATE();
		PRINT '=================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================================';
	

		PRINT '=================================================================';
		PRINT 'Loading data for cor_buss';
		PRINT '=================================================================';
	

	
				-- Loading data for flights
		SET @start_time =  GETDATE();
		PRINT '>> Truncating Table: bronze.flights';
		TRUNCATE TABLE bronze.flights;

		PRINT '>> Inserting Data Into: bronze.flight';
		BULK INSERT bronze.flights
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_11\dataset\cor_buss\flights.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------';


				-- Loading data for passengers
		SET @start_time =  GETDATE();
		PRINT '>> Truncating Table: bronze.passengers';
		TRUNCATE TABLE bronze.passengers;

		PRINT '>> Inserting Data Into: bronze.passengers';
		BULK INSERT bronze.passengers
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_11\dataset\cor_buss\passengers.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------';

	

				-- Loading data for tickets
		SET @start_time =  GETDATE();
		PRINT '>> Truncating Table: bronze.tickets';
		TRUNCATE TABLE bronze.tickets;

		PRINT '>> Inserting Data Into: bronze.tickets';
		BULK INSERT bronze.tickets
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_11\dataset\cor_buss\tickets.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------';
		PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------';



		PRINT '=================================================================';
		PRINT 'Loading data for air_ops';
		PRINT '=================================================================';
	


			-- Loading data for payments
		SET @start_time =  GETDATE();
		PRINT '>> Truncating Table: bronze.payments';	
		TRUNCATE TABLE bronze.payments;

		PRINT '>> Inserting Data Into: bronze.payments';
		BULK INSERT bronze.payments
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_11\dataset\air_ops\payments.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------';


			-- Loading data for operations
		SET @start_time =  GETDATE();
		PRINT '>> Truncating Table: bronze.operations';
		TRUNCATE TABLE bronze.operation;

		PRINT '>> Inserting Data Into: operations';
		BULK INSERT bronze.operations
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_11\dataset\air_ops\operations.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------';



		PRINT '=================================================================';
		PRINT 'Loading data for air_ref'
		PRINT '=================================================================';
	
		
		
			-- Loading data for airports
		SET @start_time =  GETDATE();
		PRINT '>> Truncating Table: bronze.airports';
		TRUNCATE TABLE bronze.airports;

		
		PRINT '>> Inserting Data Into: bronze.airports';
		BULK INSERT bronze.airports
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_11\dataset\ref_air\airports.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------';


			-- Loading data for airlines
		SET @start_time =  GETDATE();
		PRINT '>> Truncating Table: bronze.airlines';
		TRUNCATE TABLE bronze.airlines;

		PRINT '>> Inserting Data Into: airlines';
		BULK INSERT bronze.airlines
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_11\dataset\ref_air\airlines.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------';


		SET @batch_end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT 'Loaing Bronze Layer is completed';
		PRINT '   - Total Load Duration ' + CAST (DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '====================================================================='


	END TRY
	BEGIN CATCH
		PRINT '=================================================================='
		PRINT 'ERROR OCCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message:' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=================================================================='
	END CATCH
END

