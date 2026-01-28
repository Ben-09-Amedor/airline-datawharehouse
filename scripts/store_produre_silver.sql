    /*   
    =============================================================================================================
    Store Procedure: Load Silver Layer (Bronze -> Silver)
    =============================================================================================================
    Script Purpose: 
	    This store procedure performs ETL ( Extract, Transform, Load) process to populate data from the 'bronze'
	    Schema to the 'silver' Schema. 
	    Action Performed:
		    - Truncate Silver Table 
		    - Insert Transformed and Cleaned data from Bronze to Silver Table.

	    Parameters
	    None.
	    This store Procedure does not return any value or accepts any parameter.

	    Usage Example:
		    EXEC silver.load_silver;
    ==============================================================================================================

    */

    CREATE OR ALTER PROCEDURE silver.load_silver AS
    BEGIN 
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	    BEGIN TRY
		    SET @batch_start_time =  GETDATE();
		    PRINT '=================================================================';
		    PRINT 'Loading Silver Layer';
		    PRINT '=================================================================';
	

		    PRINT '------------------------------------------------------------------';
		    PRINT 'Loading data for cor_buss'
		    PRINT '------------------------------------------------------------------';
            
		    -- Loading silver.flights
		    SET @start_time =  GETDATE();
		    PRINT '>> Truncating Table: silver.flights';
		    TRUNCATE TABLE silver.flights;
		    PRINT '>> Inserting Data Into: silver.flights';
   
     INSERT INTO silver.flights (
             flight_id,
             airline_id,
             departure_airport,
             arrival_airport,
             scheduled_departure,
             scheduled_arrival
             
             )
     SELECT 
             flight_id,
             airline_id,
             departure_airport,
             arrival_airport,
            CAST( (CAST( scheduled_departure AS FLOAT )) AS DATETIME ) AS scheduled_departure,
            CAST( CAST(Scheduled_arrival AS FLOAT) AS DATETIME) scheduled_arrival
    FROM bronze.flights;
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>> --------------------------------';
    
   
            -- cleaning passenger table
    SET @start_time =  GETDATE();
    PRINT '>> Truncating Table: silver.passengers';
    TRUNCATE TABLE silver.passengers;
    PRINT '>> Inserting Data Into: silver.passengers';
   
    INSERT INTO silver.passengers (
             passenger_id,
             first_name,
             last_name,
             gender,
             nationality,
             birth_date
               )
    SELECT 
        passenger_id,
        REPLACE(first_name, ' ', '') AS first_name,
        REPLACE(last_name, ' ', '') AS last_name,
        CASE WHEN gender = 'M' THEN 'Male'
             WHEN gender = 'F' THEN 'Female'
             ELSE 'Undisclosed'
        END AS genders,
        CASE WHEN nationality = 'GE' THEN 'Germany'
             WHEN nationality = 'GH' THEN 'Ghana'
             WHEN nationality = 'FR' THEN 'France'
             WHEN nationality = 'US' THEN 'USA'
             ELSE nationality
        END AS nationality,
       birth_date
    FROM(
        SELECT
            passenger_id,
            CONCAT(
                UPPER(LEFT(LTRIM(RTRIM(first_name)), 1)),
                LOWER(SUBSTRING(LTRIM(RTRIM(first_name)), 2, LEN(first_name)))
               ) AS first_name,
            CONCAT(
                UPPER(LEFT(LTRIM(RTRIM(last_name)), 1)),
                LOWER(SUBSTRING(LTRIM(RTRIM(last_name)), 2, LEN(last_name)))
               ) AS last_name,
            UPPER(LTRIM(LEFT(gender,1)))AS gender,
            UPPER(LTRIM(LEFT(nationality,2 ))
            ) AS nationality,
             CAST( CAST(dob AS FLOAT) AS DATETIME) AS birth_date
    FROM bronze.passengers)t
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>> --------------------------------';


     
            -- cleaning data for ticket
    SET @start_time =  GETDATE();
    PRINT '>> Truncating Table: silver.tickets';
    TRUNCATE TABLE silver.tickets;
    PRINT '>> Inserting Data Into: silver.tickets';
   
    INSERT INTO silver.tickets (
            ticket_id,
            passenger_id,
            flight_id,
            purchase_date,
            ticket_class,
            price
            
               )
    SELECT 
        ticket_id,
        passenger_id,
        flight_id,
        purchase_date,
        REPLACE(ticket_class, ' ', '') AS ticket_class,
        COALESCE( 
        CAST(price AS INT), AVG(CAST(price AS INT)) OVER ( PARTITION BY TRIM(UPPER(ticket_class)) ) ) 
        AS price -- used average price of ticket class to fill null ticket values
    FROM(
    SELECT 
        ticket_id,
        passenger_id,
        flight_id,
        purchase_date,
        CONCAT(
            UPPER(LEFT(LTRIM(RTRIM(ticket_class)), 1)),
            LOWER(SUBSTRING(LTRIM(RTRIM(ticket_class)), 2, LEN(ticket_class)))
            ) AS ticket_class,
          REPLACE(REPLACE(LTRIM(RTRIM(price)), '"', ''), ',', '') AS price
    FROM bronze.tickets)t
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>> --------------------------------';



    /*
    ================================================================================================
					    Loading table for operations
    ==================================================================================================
    */

		    PRINT '------------------------------------------------------------------';
		    PRINT 'Loading data for operations'
		    PRINT '------------------------------------------------------------------';
           
            -- loading data for  payments
            SET @start_time =  GETDATE();
    PRINT '>> Truncating Table: silver.payments';
    TRUNCATE TABLE silver.payments;
    PRINT '>> Inserting Data Into: silver.payments';
   
    INSERT INTO silver.payments (
           payment_id,
           ticket_id,
           payment_method,
           payment_status,
           payment_date
               )
    
    SELECT
           payment_id,
           ticket_id,
        CASE 
             WHEN payment_method = 'BA' THEN 'Bank Transfer'
             WHEN payment_method = 'PA' THEN 'Paypal'
             WHEN payment_method = 'DE' THEN 'Debit Card'
             WHEN payment_method = 'CR' THEN 'Credit Card'
             WHEN payment_method = 'MO' THEN 'Mobile Money'
             ELSE 'Unknown'
        END AS payment_method,
        CASE    
             WHEN payment_status  = 'FA' THEN 'Failed'
             WHEN payment_status = 'CO' THEN 'Completed'
             WHEN payment_status = 'PE' THEN 'Pending'
             ELSE 'Unknown'
        END AS payment_status,
             payment_date
  FROM
  (SELECT
           payment_id,
           ticket_id,
           UPPER(LEFT(LTRIM(payment_method),2)) AS Payment_Method,
           UPPER(LEFT(LTRIM(payment_status),2)) AS Payment_status,
            CAST( CAST(payment_date AS FLOAT) AS DATETIME) AS payment_date
    FROM bronze.payments)t
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>> --------------------------------';

    
                 -- cleaning data for operations
                 
    SET @start_time =  GETDATE();
    PRINT '>> Truncating Table: silver.operations';
    TRUNCATE TABLE silver.operations;
    PRINT '>> Inserting Data Into: silver.operations';
   
    INSERT INTO silver.operations (
            operation_id,
            flight_id,
            cancellation_flag,
            delay_minutes,
            reason
               )
    
      SELECT
        operation_id,
        flight_id,
        
         CASE    
             WHEN cancellation_flag  = 1 THEN 'Yes'
             WHEN cancellation_flag  = 0 THEN 'No'
        END AS cancellation_flag, -- convert cancellation flag to more readable 
        delay_minutes,
        LTRIM(reason) AS reason
    FROM bronze.operations
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>> --------------------------------';



            PRINT '------------------------------------------------------------------';
		    PRINT 'Loading data for refrence'
		    PRINT '------------------------------------------------------------------';


            -- cleaning data for airlines
   SET @start_time =  GETDATE();
    PRINT '>> Truncating Table: silver.airline';
    TRUNCATE TABLE silver.airline
    PRINT '>> Inserting Data Into: airlines';
   
    INSERT INTO silver.airlines(
           airline_id,
           airline_name,
           country
           
               )
    SELECT
        airline_id,
        CASE 
             WHEN airline_name = 'TR' THEN 'TransWorld'
             WHEN airline_name = 'AE' THEN 'AeroFly'
             WHEN airline_name = 'SK' THEN 'SkyJet'
             WHEN airline_name = 'BL' THEN 'Blue Skies'
             WHEN airline_name=  'GL' THEN 'Global Air'
        END AS airline_name,
        CASE 
             WHEN country = 'JA'  THEN 'Japan'
             WHEN country = 'GE'  THEN 'Germany'
             WHEN country = 'US'  THEN 'USA'
             WHEN country = 'GH'  THEN 'Ghana'  
             WHEN country = 'UK'  THEN 'UK' 
             WHEN country = 'UA'  THEN 'UAE'
             WHEN country = 'FR'  THEN 'France'
             ELSE country
        END AS country
     FROM(
     SELECT 
        airline_id,
        UPPER(LEFT(LTRIM(airline_name),2)) AS airline_name,
        UPPER(LEFT(LTRIM(Country), 2)) AS country
    FROM bronze.airlines)t
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>> --------------------------------';
   

   select*from bronze.airports
            --- loading data for airports
    SET @start_time =  GETDATE();
    PRINT '>> Truncating Table: silver.airports';
    TRUNCATE TABLE silver.airports
    PRINT '>> Inserting Data Into: silver.airports';
   
    INSERT INTO silver.airports (
            airport_code,
            city,
            country
               )
    SELECT
        airport_code,
        CASE 
             WHEN country = 'Germany' AND city != 'Berlin' THEN 'Berlin' 
             WHEN country = 'Ghana' AND city != 'Accra' THEN 'Accra' 
             WHEN country = 'USA' AND city != 'New York' THEN 'New York' 
             WHEN country = 'France' AND city != 'Paris' THEN 'Paris' 
             WHEN country = 'UK' AND city != 'London' THEN 'London'
             WHEN country = 'Japan' AND city != 'Tokyo' THEN 'London'
             WHEN country = 'UAE' AND city != 'Dubai' THEN 'Dubai'
             ELSE city 
        END AS city,
        country
    FROM(
    SELECT
        airport_code,
        CASE WHEN city = 'DU' THEN 'Dubai'
             WHEN city = 'TO' THEN 'Tokyo'
             WHEN city = 'AC' THEN 'Accra'
             WHEN city = 'NE' THEN 'New York'
             WHEN city = 'LO' THEN 'London'
             WHEN city = 'BE' THEN 'Berlin'
             WHEN city = 'PA' THEN 'Paris'
        END AS city,
        CASE 
             WHEN country = 'JA'  THEN 'Japan'
             WHEN country = 'GE'  THEN 'Germany'
             WHEN country = 'US'  THEN 'USA'
             WHEN country = 'GH'  THEN 'Ghana'  
             WHEN country = 'UK'  THEN 'UK' 
             WHEN country = 'UA'  THEN 'UAE'
             WHEN country = 'FR'  THEN 'France'
             ELSE country
        END AS country
    FROM(
    SELECT
        airport_code,
        UPPER(LEFT(LTRIM(city),2)) AS city,
        UPPER(LEFT(LTRIM(Country), 2)) AS country
        FROM bronze.airports)t
        )t
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>> --------------------------------';


    SET @batch_end_time = GETDATE();
	     PRINT '>> Load Duration:' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	     PRINT 'Loaing silver Layer is completed';
	     PRINT '   - Total Load Duration ' + CAST (DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	     PRINT '=====================================================================';
        END TRY
	
	
	   BEGIN CATCH
		    PRINT '==================================================================';
		    PRINT 'ERROR OCCCURED DURING LOADING SILVER LAYER'
		    PRINT 'Error Message:' + ERROR_MESSAGE();
		    PRINT 'Error Message:' + CAST (ERROR_NUMBER() AS NVARCHAR);
		    PRINT 'Error Message:' + CAST (ERROR_STATE() AS NVARCHAR);
		    PRINT '==================================================================';
	    END CATCH


    END


