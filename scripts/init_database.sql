/*
Create Database and Schemas
Script Purpose:
This script creates the Data Warehouse database and the required schemas:
- bronze
- silver
- gold
*/

-- Create the Database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
CREATE DATABASE DataWarehouse;
END
GO

USE DataWarehouse;
GO

-- Create the Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
