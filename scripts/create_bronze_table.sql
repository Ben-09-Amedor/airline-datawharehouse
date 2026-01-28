	/*
	===================================================================================
	DDL Scripts: Create Bronze Table
	===================================================================================
	Script Purpose:
	This script creates bronze Table. It will drop existing table if they already exist
	Run this scripts to re-define the DDL structure of 'bronze' Table

	====================================================================================

	*/


	/*
	================================================================================================
						create table for core business
	==================================================================================================
	*/
	-- Create table for flights
	IF OBJECT_ID('bronze.flights', 'U') IS NOT NULL
		DROP TABLE bronze.flights
	CREATE TABLE bronze.flights(
			flight_id					INT,
			airline_id					INT,
			departure_airport			NVARCHAR(10),
			arrival_airport				NVARCHAR(10),
			scheduled_departure			NVARCHAR(50),
			Scheduled_arrival			NVARCHAR(50)

			);


	-- Create table for passenger
	IF OBJECT_ID('bronze.passengers', 'U') IS NOT NULL
		DROP TABLE bronze.passengers
	 CREATE TABLE bronze.passengers(
			passenger_id				INT,
			first_name					NVARCHAR(50),
			last_name					NVARCHAR(50),
			gender						NVARCHAR(20),
			nationality					NVARCHAR(10),
			dob							NVARCHAR(50)


			);



	-- Create table for tickets
	IF OBJECT_ID ('bronze.tickets', 'U') IS NOT NULL
		DROP TABLE bronze.tickets
	CREATE TABLE bronze.tickets(
			ticket_id					INT,
			passenger_id				INT,
			flight_id					INT,
			purchase_date				DATETIME,
			ticket_class				NVARCHAR(20),
			Price						NVARCHAR(20)
			

			);



		
	/*
	================================================================================================
						create table for operations
	==================================================================================================
	*/
	-- Create table for payments
	IF OBJECT_ID('bronze.payemts', 'U') IS NOT NULL
		DROP TABLE bronze.payments
	 CREATE TABLE bronze.payments (
			payment_id				INT,
			ticket_id				NVARCHAR(10),
			Payment_method			NVARCHAR(50),
			payment_status			NVARCHAR(50),
			payment_date			NVARCHAR(50)
		
			);



	-- Create table for delays operation
	IF OBJECT_ID('bronze.operations', 'U') IS NOT NULL
		DROP TABLE bronze.operations
	CREATE TABLE bronze.operations(
		   operation_id					INT,
		   flight_id					INT,
		   cancellation_flag			NVARCHAR(50),
		   delay_minutes				INT,
		   reason						NVARCHAR(50)


		   );

	/*
	================================================================================================
						create table for refrence
	==================================================================================================
	*/

			-- create table for airlines
	IF OBJECT_ID('bronze.airlines', 'U') IS NOT NULL
		DROP TABLE bronze.airlines;

	CREATE TABLE bronze.airlines(
			airline_id					INT,
			airline_name				NVARCHAR(50),
			Country						NVARCHAR(50)
		);


		-- craete table for airports
		IF OBJECT_ID('bronze.airports', 'U') IS NOT NULL
			DROP TABLE bronze.airports;

		CREATE TABLE bronze.airports(
				airport_code			NVARCHAR(20),
				city					NVARCHAR(50),
				Country					NVARCHAR(50)


				);
