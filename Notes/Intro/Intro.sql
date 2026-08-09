/*==== TOPIC : SQL - Introduction ====*/

/*
===============================================================================
 1. WHAT IS DATABASE AND SQL
===============================================================================

 DATABASE
 --------
 A Database is an organized collection of data stored electronically,
 so that it can be easily accessed, managed and updated.

 - Data is stored in TABLES (rows and columns).
 - ROW    -> one record          (one customer)
 - COLUMN -> one attribute/field (customer name)

 Example table: customers
 +-------------+---------------+---------------+
 | customer_id | customer_name | country       |
 +-------------+---------------+---------------+
 | 1           | Utkarsh       | India         |
 | 2           | John          | USA           |
 +-------------+---------------+---------------+


 SQL (Structured Query Language)
 -------------------------------
 SQL is the standard language used to COMMUNICATE with a database.
 We use SQL to create, read, update and delete data (CRUD).

 - It is a DECLARATIVE language -> we tell WHAT we want, not HOW to get it.
 - Pronounced as "S-Q-L" or "Sequel".
*/

-- Simple SQL query: read all rows from a table
SELECT * FROM customers;

-- Read specific columns with a filter
SELECT customer_name, country
FROM customers
WHERE country = 'India';


/*
===============================================================================
 2. WHAT IS DBMS AND SQL SERVER
===============================================================================

 DBMS (Database Management System)
 ---------------------------------
 DBMS is the SOFTWARE that manages the database.
 It sits between the USER and the DATABASE.

        USER  --->  DBMS  --->  DATABASE (files on disk)

 Main jobs of a DBMS:
 - Store and retrieve data
 - Security (who can access what)
 - Backup and recovery
 - Handle many users at the same time (concurrency)
 - Keep data correct and consistent (integrity)

 RDBMS (Relational DBMS)
 -----------------------
 A DBMS that stores data in RELATED TABLES and uses SQL.
 Relations are made using KEYS:
   - PRIMARY KEY -> uniquely identifies a row in a table
   - FOREIGN KEY -> points to the primary key of another table

 Popular RDBMS: SQL Server, MySQL, PostgreSQL, Oracle, SQLite


 SQL SERVER
 ----------
 Microsoft SQL Server is an RDBMS product developed by Microsoft.
 - Its SQL dialect is called T-SQL (Transact-SQL)
   = standard SQL + extras (variables, IF/ELSE, loops, error handling).
 - Managed using SSMS (SQL Server Management Studio) or Azure Data Studio.
 - Default port: 1433

 IMPORTANT: SQL is the LANGUAGE, SQL Server is the SOFTWARE (product).
*/

-- T-SQL example (variables + IF, not available in plain standard SQL)
DECLARE @total INT;
SELECT @total = COUNT(*) FROM customers;

IF @total > 0
    PRINT 'Table has data';
ELSE
    PRINT 'Table is empty';


/*
===============================================================================
 3. TYPES OF DATABASE
===============================================================================

 A) RELATIONAL DATABASE (SQL)
    - Data in tables (rows + columns), fixed schema.
    - Uses SQL, supports ACID transactions.
    - Example: SQL Server, MySQL, PostgreSQL, Oracle
    - Use for: banking, ERP, structured business data

 B) NON-RELATIONAL DATABASE (NoSQL)
    - Flexible / no fixed schema, built to scale horizontally.
    - 4 main types:
        1. Document   -> JSON like documents   (MongoDB)
        2. Key-Value  -> simple key : value    (Redis)
        3. Column     -> wide column store     (Cassandra)
        4. Graph      -> nodes and relations   (Neo4j)
    - Use for: real-time apps, big data, social networks

 C) BY WORKLOAD (very important for Data Warehouse)
    1. OLTP (Online Transaction Processing)
       - Day to day operations, many small INSERT/UPDATE/DELETE
       - Highly normalized, fast writes
       - Example: an e-commerce order system

    2. OLAP (Online Analytical Processing)
       - Analysis and reporting, heavy SELECT on huge data
       - Denormalized (Star Schema: fact + dimension tables)
       - Example: Data Warehouse (this project)

 D) OTHER COMMON TYPES
    - Data Warehouse -> central store of cleaned, historical data for analytics
    - Data Lake      -> raw data in any format (files, logs, images)
    - Cloud Database -> hosted on cloud (Azure SQL, Amazon RDS, Snowflake)
    - In-Memory DB   -> data kept in RAM for speed (Redis, SAP HANA)


 ACID (properties of a reliable transaction)
 -------------------------------------------
 A - Atomicity   : all steps happen, or none
 C - Consistency : database stays in a valid state
 I - Isolation   : transactions do not disturb each other
 D - Durability  : committed data is never lost
*/


/*
===============================================================================
 4. TYPES OF SQL COMMANDS
===============================================================================

 SQL commands are grouped into 5 categories:

   DDL -> Data Definition Language   (structure)
   DML -> Data Manipulation Language (data)
   DQL -> Data Query Language        (read)
   DCL -> Data Control Language      (permissions)
   TCL -> Transaction Control Language (transactions)
*/


-- ----------------------------------------------------------------------------
-- 4.1 DDL - DATA DEFINITION LANGUAGE  (defines structure, AUTO-COMMIT)
--     CREATE | ALTER | DROP | TRUNCATE | RENAME
-- ----------------------------------------------------------------------------

-- CREATE : make a new database / table / view
CREATE TABLE customers (
    customer_id   INT PRIMARY KEY,
    customer_name VARCHAR(50) NOT NULL,
    country       VARCHAR(50),
    create_date   DATE
);

-- ALTER : change structure of an existing table
ALTER TABLE customers ADD email VARCHAR(100);
ALTER TABLE customers DROP COLUMN email;

-- TRUNCATE : delete ALL rows, keep the table structure (fast, no WHERE)
TRUNCATE TABLE customers;

-- DROP : delete the whole table (structure + data)
DROP TABLE customers;


-- ----------------------------------------------------------------------------
-- 4.2 DML - DATA MANIPULATION LANGUAGE  (works on data, can be ROLLED BACK)
--     INSERT | UPDATE | DELETE
-- ----------------------------------------------------------------------------

-- INSERT : add new rows
INSERT INTO customers (customer_id, customer_name, country)
VALUES (1, 'Utkarsh', 'India');

-- UPDATE : change existing rows (always use WHERE!)
UPDATE customers
SET country = 'USA'
WHERE customer_id = 1;

-- DELETE : remove specific rows
DELETE FROM customers
WHERE customer_id = 1;


-- ----------------------------------------------------------------------------
-- 4.3 DQL - DATA QUERY LANGUAGE  (read data)
--     SELECT
-- ----------------------------------------------------------------------------

SELECT country,
       COUNT(*) AS total_customers
FROM customers
WHERE country IS NOT NULL      -- 1. filter rows
GROUP BY country               -- 2. make groups
HAVING COUNT(*) > 1            -- 3. filter groups
ORDER BY total_customers DESC; -- 4. sort result


-- ----------------------------------------------------------------------------
-- 4.4 DCL - DATA CONTROL LANGUAGE  (permissions / security)
--     GRANT | REVOKE | DENY
-- ----------------------------------------------------------------------------

GRANT SELECT ON customers TO analyst_user;   -- give permission
REVOKE SELECT ON customers FROM analyst_user;-- take back permission
DENY  DELETE ON customers TO analyst_user;   -- block permission (SQL Server)


-- ----------------------------------------------------------------------------
-- 4.5 TCL - TRANSACTION CONTROL LANGUAGE  (manage a group of DML statements)
--     BEGIN TRANSACTION | COMMIT | ROLLBACK | SAVE TRANSACTION
-- ----------------------------------------------------------------------------

BEGIN TRANSACTION;

    UPDATE customers SET country = 'UK' WHERE customer_id = 1;

    -- ROLLBACK;  -- undo all changes in this transaction
COMMIT;           -- save all changes permanently


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Database  -> place where data is stored (tables)
 SQL       -> language to talk to the database
 DBMS      -> software that manages the database
 RDBMS     -> DBMS using related tables + SQL
 SQL Server-> Microsoft's RDBMS, uses T-SQL

 DDL -> CREATE, ALTER, DROP, TRUNCATE      -> structure
 DML -> INSERT, UPDATE, DELETE             -> data
 DQL -> SELECT                             -> read
 DCL -> GRANT, REVOKE, DENY                -> permission
 TCL -> COMMIT, ROLLBACK, SAVEPOINT        -> transaction

 DELETE vs TRUNCATE vs DROP
 --------------------------
 DELETE   -> DML, removes selected rows, can rollback, slow
 TRUNCATE -> DDL, removes all rows, keeps table, fast
 DROP     -> DDL, removes table completely (data + structure)
===============================================================================
*/
