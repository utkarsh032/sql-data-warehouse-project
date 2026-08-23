/*==== TOPIC : SQL - DML (Data Manipulation Language) ====*/

/*
===============================================================================
 1. WHAT IS DML
===============================================================================

 DML = Data Manipulation Language.
 It works on the DATA (rows) inside a table, not on the table structure.

 Commands:
   INSERT -> add new rows
   UPDATE -> change existing rows
   DELETE -> remove rows
   (SELECT is sometimes counted here, but usually called DQL)

 IMPORTANT
 - DML is NOT auto-commit -> it can be undone with ROLLBACK.
 - UPDATE and DELETE without WHERE affect the WHOLE table. Be careful!
 - Golden rule: first run it as a SELECT, check the rows, then change
   SELECT to UPDATE / DELETE.

 DDL vs DML
   DDL -> CREATE / ALTER / DROP  -> structure of the table
   DML -> INSERT / UPDATE/ DELETE-> data inside the table
*/

-- Sample table used in this file
CREATE TABLE customers (
    customer_id   INT PRIMARY KEY,
    customer_name VARCHAR(50),
    country       VARCHAR(50),
    score         INT
);


/*
===============================================================================
 2. INSERT  -> add new rows
===============================================================================

 Syntax:
   INSERT INTO table (col1, col2) VALUES (val1, val2);

 Rules
 - Column list and value list must match in NUMBER and ORDER.
 - Text and dates go inside single quotes ' '.
 - Columns not mentioned get their DEFAULT value, or NULL.
*/

-- ---- 1) Insert one row (with column names - RECOMMENDED) ----
INSERT INTO customers (customer_id, customer_name, country, score)
VALUES (1, 'Utkarsh', 'India', 750);

-- ---- 2) Insert without column names ----
-- Works only if you give a value for EVERY column in the exact table order.
-- Not recommended, it breaks when the table changes.
INSERT INTO customers
VALUES (2, 'John', 'USA', 900);

-- ---- 3) Insert MANY rows in one statement ----
INSERT INTO customers (customer_id, customer_name, country, score)
VALUES (3, 'Maria', 'Germany', 500),
       (4, 'Peter', 'UK',      650),
       (5, 'Anna',  'India',   400);

-- ---- 4) Insert only some columns (rest become NULL / DEFAULT) ----
INSERT INTO customers (customer_id, customer_name)
VALUES (6, 'Sam');

-- ---- 5) INSERT ... SELECT : copy rows from another table ----
-- Used a lot in ETL: move data from bronze layer to silver layer.
INSERT INTO customers_backup (customer_id, customer_name, country, score)
SELECT customer_id, customer_name, country, score
FROM   customers
WHERE  country = 'India';

-- ---- 6) SELECT ... INTO : create a NEW table and copy data into it ----
-- The new table must NOT already exist.
SELECT customer_id, customer_name
INTO   customers_india
FROM   customers
WHERE  country = 'India';

-- NOTE: with IDENTITY columns, do not insert the id yourself,
--       SQL Server generates it automatically.


/*
===============================================================================
 3. UPDATE  -> change existing rows
===============================================================================

 Syntax:
   UPDATE table
   SET    col1 = value1, col2 = value2
   WHERE  condition;

 WARNING: no WHERE = every row in the table is updated.
*/

-- ---- 1) Update one column of one row ----
UPDATE customers
SET    score = 800
WHERE  customer_id = 1;

-- ---- 2) Update multiple columns at once ----
UPDATE customers
SET    score   = 850,
       country = 'USA'
WHERE  customer_id = 1;

-- ---- 3) Update many rows with one condition ----
UPDATE customers
SET    score = 0
WHERE  score IS NULL;

-- ---- 4) Update using the column's own value (calculation) ----
UPDATE customers
SET    score = score + 100
WHERE  country = 'India';

-- ---- 5) Update using a function / expression ----
UPDATE customers
SET    customer_name = UPPER(TRIM(customer_name));

-- ---- 6) Update using data from ANOTHER table (UPDATE with JOIN) ----
UPDATE c
SET    c.country = s.country
FROM   customers c
INNER  JOIN staging_customers s
        ON c.customer_id = s.customer_id;

-- ---- 7) SAFE UPDATE PATTERN ----
-- Step 1: check which rows will change
SELECT * FROM customers WHERE customer_id = 10;
-- Step 2: run the update
UPDATE customers SET score = 500 WHERE customer_id = 10;
-- Step 3: verify
SELECT * FROM customers WHERE customer_id = 10;


/*
===============================================================================
 4. DELETE  -> remove rows
===============================================================================

 Syntax:
   DELETE FROM table WHERE condition;

 WARNING: no WHERE = all rows are deleted (structure stays).
*/

-- ---- 1) Delete one row ----
DELETE FROM customers
WHERE customer_id = 6;

-- ---- 2) Delete many rows ----
DELETE FROM customers
WHERE score < 500;

-- ---- 3) Delete rows with NULL ----
DELETE FROM customers
WHERE country IS NULL;

-- ---- 4) Delete ALL rows (slow, can rollback) ----
DELETE FROM customers;

-- ---- 5) TRUNCATE : faster way to delete all rows (DDL) ----
TRUNCATE TABLE customers;

-- ---- 6) SAFE DELETE PATTERN ----
SELECT * FROM customers WHERE score < 500;   -- check first
DELETE FROM customers WHERE score < 500;     -- then delete

-- NOTE: a row cannot be deleted if a FOREIGN KEY in a child table
--       still points to it. Delete the child rows first.


/*
===============================================================================
 5. DML WITH TRANSACTIONS  (undo option)
===============================================================================

 A transaction groups DML statements so they all succeed or all fail.
*/

BEGIN TRANSACTION;

    UPDATE customers SET score = 1000 WHERE customer_id = 1;
    DELETE FROM customers WHERE customer_id = 5;

    -- check the result before saving
    SELECT * FROM customers;

-- ROLLBACK;   -- undo everything above
COMMIT;        -- save everything above permanently


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Command   | Purpose            | WHERE | Rollback | Speed
 ----------|--------------------|-------|----------|-------
 INSERT    | add rows           |  no   |   yes    | fast
 UPDATE    | change rows        |  yes  |   yes    | medium
 DELETE    | remove rows        |  yes  |   yes    | slow
 TRUNCATE  | remove all rows    |  no   |   no*    | very fast (DDL)

 Remember
 - Always write WHERE with UPDATE and DELETE.
 - Test with SELECT before you run UPDATE / DELETE.
 - INSERT INTO ... SELECT is the main tool for loading a data warehouse.
===============================================================================
*/
