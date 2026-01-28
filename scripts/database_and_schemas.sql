/*
=====================================================================
Create Database  and Schemas 
=====================================================================

Script Purpose:
This scripts create a database named "AirlineManagement". It has three schemas; bronze, silver and gold.
It checks if the database already exists. If it exists, it drops and recreate.


Warning: 
Running this scripts will  drop 'AirlineManagement' and permanently delete all data in the database.
Proceed with caution and ensure you data is backed up before running this scripts.

*/



USE master
GO;


IF EXISTS ( SELECT 1 FROM sys.databases WHERE name = 'AirlineManagement')
	BEGIN 
	ALTER DATABASE AirlineManagement SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE AirlineManagement;
	END;


CREATE DATABASE AirlineManagement;


USE AirlineManagement;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO


CREATE SCHEMA gold;
GO
