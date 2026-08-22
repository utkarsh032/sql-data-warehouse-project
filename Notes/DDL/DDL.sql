/*==== TOPIC : SQL - DDL (Data Definition Language) ====*/

/*
===============================================================================
 1. WHAT IS DDL
===============================================================================

 DDL = Data Definition Language.
 It defines and changes the STRUCTURE (schema) of database objects,
 not the data inside them.

 Objects we create with DDL:
   DATABASE, SCHEMA, TABLE, VIEW, INDEX, PROCEDURE

 DDL commands:
   CREATE   -> make a new object
   ALTER    -> change an existing object
   DROP     -> delete an object completely
   TRUNCATE -> remove all rows, keep the empty table
   RENAME   -> change the name of an object (sp_rename in SQL Server)

 IMPORTANT
 - DDL is AUTO-COMMIT -> the change is saved immediately.
 - In SQL Server DDL can be put inside a transaction and rolled back,
   but in many other databases (Oracle, MySQL) it CANNOT be rolled back.
 - DDL locks the object while it runs, so avoid it on busy production tables.

 DDL vs DML
   DDL -> works on the STRUCTURE (the table itself)
   DML -> works on the DATA      (the rows inside the table)
*/


/*
===============================================================================
 2. COMMON DATA TYPES (SQL Server)
===============================================================================

 NUMBER
   INT            -> whole number
   BIGINT         -> very large whole number
   DECIMAL(p,s)   -> exact decimal, p = total digits, s = digits after point
   FLOAT          -> approximate decimal

 TEXT
   CHAR(n)        -> fixed length text  (always uses n characters)
   VARCHAR(n)     -> variable length text (uses only what is needed)
   NVARCHAR(n)    -> variable length UNICODE text (other languages, emoji)
   VARCHAR(MAX)   -> very long text

 DATE / TIME
   DATE           -> 2024-01-31
   DATETIME2      -> date + time (recommended over old DATETIME)
   TIME           -> only time

 OTHER
   BIT            -> 0 / 1 (used as true / false)
   UNIQUEIDENTIFIER -> GUID
*/


/*
===============================================================================
 3. CONSTRAINTS (rules applied on columns)
===============================================================================

 PRIMARY KEY -> unique + not null, identifies each row (only one per table)
 FOREIGN KEY -> links to the primary key of another table
 NOT NULL    -> value must be given
 UNIQUE      -> no duplicate values allowed
 CHECK       -> value must satisfy a condition
 DEFAULT     -> value used when nothing is supplied
 IDENTITY    -> auto increasing number (SQL Server auto number)
*/


/*
===============================================================================
 4. CREATE  -> make a new object
===============================================================================
*/

-- ---- CREATE DATABASE ----
CREATE DATABASE DataWarehouse;

-- ---- CREATE SCHEMA ----
-- A schema is a folder/group inside a database, used to organize tables.
-- This project uses: bronze (raw), silver (clean), gold (final)
CREATE SCHEMA bronze;
GO                                   -- GO = batch separator in SSMS

-- ---- CREATE TABLE (simple) ----
CREATE TABLE customers (
    customer_id   INT,
    customer_name VARCHAR(50),
    country       VARCHAR(50)
);

-- ---- CREATE TABLE (with constraints) ----
CREATE TABLE customers (
    customer_id   INT          IDENTITY(1,1) PRIMARY KEY,  -- auto number 1,2,3...
    customer_name VARCHAR(50)  NOT NULL,                   -- must be filled
    email         VARCHAR(100) UNIQUE,                     -- no duplicates
    score         INT          CHECK (score >= 0),         -- must be 0 or more
    country       VARCHAR(50)  DEFAULT 'Unknown',          -- used if not given
    create_date   DATE         DEFAULT GETDATE()           -- today's date
);

-- ---- CREATE TABLE (with FOREIGN KEY) ----
CREATE TABLE orders (
    order_id    INT PRIMARY KEY,
    customer_id INT,
    order_date  DATE,
    amount      DECIMAL(10,2),
    CONSTRAINT fk_orders_customers
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- ---- CREATE TABLE inside a schema ----
CREATE TABLE bronze.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE
);

-- ---- SAFE CREATE : drop the table first if it already exists ----
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info (
    cst_id  INT,
    cst_key NVARCHAR(50)
);
GO
-- 'U' means User Table. This pattern is used in the project's ddl scripts.

-- ---- CREATE VIEW : a saved query, behaves like a virtual table ----
CREATE VIEW gold.dim_customers AS
SELECT customer_id,
       customer_name,
       country
FROM   silver.crm_cust_info;
GO

-- ---- CREATE INDEX : makes searching faster on a column ----
CREATE INDEX idx_customers_country ON customers(country);

-- ---- CREATE TABLE from another table (copy structure + data) ----
SELECT customer_id, customer_name
INTO   customers_backup
FROM   customers;


/*
===============================================================================
 5. ALTER  -> change an existing object
===============================================================================
*/

-- ---- ADD a column ----
ALTER TABLE customers
ADD phone VARCHAR(15);

-- ---- ADD multiple columns at once ----
ALTER TABLE customers
ADD city VARCHAR(50),
    zip  VARCHAR(10);

-- ---- DROP (remove) a column ----
ALTER TABLE customers
DROP COLUMN phone;

-- ---- CHANGE data type or nullability of a column ----
-- In SQL Server the keyword is ALTER COLUMN
-- (MySQL uses MODIFY, Oracle uses MODIFY)
ALTER TABLE customers
ALTER COLUMN customer_name VARCHAR(100) NOT NULL;

-- ---- ADD a constraint ----
ALTER TABLE customers
ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

ALTER TABLE customers
ADD CONSTRAINT chk_score CHECK (score >= 0);

ALTER TABLE orders
ADD CONSTRAINT fk_orders_customers
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id);

-- ---- DROP a constraint ----
ALTER TABLE customers
DROP CONSTRAINT chk_score;

-- ---- RENAME a column / table (system procedure, not ALTER) ----
EXEC sp_rename 'customers.customer_name', 'cust_name', 'COLUMN';
EXEC sp_rename 'customers', 'dim_customers';

-- NOTE: ALTER cannot rename a column directly in SQL Server, use sp_rename.


/*
===============================================================================
 6. DROP  -> delete an object completely
===============================================================================

 DROP removes both the STRUCTURE and the DATA. It cannot be undone
 (unless it is inside an open transaction in SQL Server).
*/

-- ---- DROP TABLE ----
DROP TABLE customers;

-- ---- Safe drop : only if it exists (SQL Server 2016+) ----
DROP TABLE IF EXISTS customers;

-- ---- Older / project style safe drop ----
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

-- ---- DROP other objects ----
DROP VIEW  IF EXISTS gold.dim_customers;
DROP INDEX idx_customers_country ON customers;
DROP SCHEMA bronze;                 -- schema must be empty first
DROP DATABASE DataWarehouse;        -- nobody should be connected to it

-- NOTE: a table cannot be dropped if another table's FOREIGN KEY points to it.
--       Drop the child table (or the constraint) first.


/*
===============================================================================
 7. TRUNCATE  -> empty the table, keep the structure
===============================================================================
*/

TRUNCATE TABLE bronze.crm_cust_info;

-- Used in this project before every full load:
-- TRUNCATE the bronze table, then BULK INSERT the fresh CSV data.


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Command   | What it does                    | Data | Structure | Rollback
 ----------|---------------------------------|------|-----------|----------
 CREATE    | make new object                 |  -   | created   | no*
 ALTER     | change structure of object      | kept | changed   | no*
 DROP      | delete object completely        | gone | gone      | no*
 TRUNCATE  | delete all rows, keep table     | gone | kept      | no*
 DELETE    | delete selected rows (DML)      | rows | kept      | YES

 * SQL Server allows rollback if wrapped in an explicit transaction,
   most other databases do not.

 DELETE vs TRUNCATE vs DROP
   DELETE   -> DML, WHERE allowed, row by row, slow, logs each row
   TRUNCATE -> DDL, no WHERE, resets IDENTITY back to 1, very fast
   DROP     -> DDL, table itself disappears

 Order to remember: CREATE it -> ALTER it -> TRUNCATE it -> DROP it
===============================================================================
*/
