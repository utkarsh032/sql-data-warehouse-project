# Create Database and Schemas

## Description
This script creates a new **SQL Server database** named **DataWarehouse** and initializes the **Bronze, Silver, and Gold schemas** used in the **Medallion Architecture** for data warehousing.

---

## ⚠️ Warning

Running this script will **drop the entire `DataWarehouse` database if it already exists**.

All data inside the database will be **permanently deleted**.

Ensure you **have proper backups** before executing this script.

---

## SQL Script

```sql
/*
==============================================
Create Database and Schemas
==============================================

Script Purpose:
This script creates a new database named 'DataWarehouse'.
If the database already exists, it will be dropped and recreated.

It also creates three schemas used in the Medallion Architecture:
- bronze : Raw data layer
- silver : Cleaned and transformed data
- gold   : Business-ready analytical data
*/

USE master;
GO

-- Drop the database if it already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse 
    SET SINGLE_USER 
    WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;
END
GO

-- Create new database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Bronze Schema (Raw Layer)
CREATE SCHEMA bronze;
GO

-- Create Silver Schema (Cleaned Layer)
CREATE SCHEMA silver;
GO

-- Create Gold Schema (Business Layer)
CREATE SCHEMA gold;
GO
