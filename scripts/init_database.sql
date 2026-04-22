-- Create Database 'DataWarehouse'

/*
=============================================
Create Database and Schemas
=============================================
Script Purpose: 
This script create a new database called 'DataWarehouse' 
and defines three schemas. If the database already exists, it will be dropped and recreated.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' Database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' Database
CREATE DATABASE DataWarehouse;
USE DataWarehouse;

-- Create Schemas for the DataWarehouse
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
