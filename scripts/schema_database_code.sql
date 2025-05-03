-- Create Database

USE master;
GO 
--drop & recreate the 'datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.database WHERE name = 'DataWarehouse')
BEGIN 
ALTER DATABSE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END 
GO
--CREATE THE 'datawarehouse' database
CREATE DATABASE DataWarehouse;

Use DataWarehouse
GO
--CREATE THE SCHEMAS
Create SCHEMA bronze;
GO
Create SCHEMA silver;
GO
Create SCHEMA gold;
