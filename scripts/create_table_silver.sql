/*
===================================================================================
DDL Scripts: Create silver Table
===================================================================================
Script Purpose:
This script creates silver Table. It will drop existing table if they already exist
Run this scripts to re-define the DDL structure of 'silver' Table

====================================================================================

*/


/*
================================================================================================
					create table for cor_buss
==================================================================================================
*/
			-- Create table for flights
IF OBJECT_ID('silver.flights', 'U') IS NOT NULL
	DROP TABLE silver.flights
CREATE TABLE silver.flights(
		flight_id					INT,
		airline_id					INT,
		departure_airport			NVARCHAR(10),
		arrival_airport				NVARCHAR(10),
		scheduled_departure			DATE,
		Scheduled_arrival			DATE
		
		);

		
				-- Create table for passenger
IF OBJECT_ID('silver.passengers', 'U') IS NOT NULL
	DROP TABLE silver.passengers
 CREATE TABLE silver.passengers(
		passenger_id				INT,
		first_name					NVARCHAR(50),
		last_name					NVARCHAR(50),
		gender						NVARCHAR(20),
		nationality					NVARCHAR(10),
		birth_date					DATE


		);



		-- Create table for tickets
IF OBJECT_ID ('silver.tickets', 'U') IS NOT NULL
	DROP TABLE silver.tickets
CREATE TABLE silver.tickets(
		ticket_id					INT,
		passenger_id				INT,
		flight_id					INT,
		purchase_date				DATE,
		ticket_class				NVARCHAR(20),
		Price						INT
		

		);



	
/*
================================================================================================
					create table for operations
==================================================================================================
*/
			-- Create table for payments
IF OBJECT_ID('silver.payemts', 'U') IS NOT NULL
	DROP TABLE silver.payments
 CREATE TABLE silver.payments (
		payment_id				INT,
		ticket_id				NVARCHAR(10),
		Payment_method			NVARCHAR(50),
		payment_status			NVARCHAR(50),
		payment_date			DATE
	
		);



			-- Create table for delays operation
IF OBJECT_ID('silver.operations', 'U') IS NOT NULL
	DROP TABLE silver.operations
CREATE TABLE silver.operations(
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
IF OBJECT_ID('silver.airlines', 'U') IS NOT NULL
	DROP TABLE silver.airlines;

CREATE TABLE silver.airlines(
		airline_id					INT,
		airline_name				NVARCHAR(50),
		Country						NVARCHAR(50)
	);

			-- create table for airports
	IF OBJECT_ID('silver.airports', 'U') IS NOT NULL
		DROP TABLE silver.airports;

	CREATE TABLE silver.airports(
			airport_code			NVARCHAR(20),
			city					NVARCHAR(50),
			country					NVARCHAR(50)


			);
