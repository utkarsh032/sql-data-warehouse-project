/*==== TOPIC : SQL - SET OPERATORS ====*/

/*
===============================================================================
 1. INTRO TO SET OPERATORS
===============================================================================

 SET OPERATORS combine the RESULTS of two or more SELECT queries
 into ONE result set.

 JOIN vs SET OPERATOR
 --------------------
 JOIN          -> combines COLUMNS (horizontally, side by side), needs a KEY
 SET OPERATOR  -> combines ROWS    (vertically, one below the other), no KEY

      JOIN                    SET OPERATOR
      A | B                        A
      --+--                        -
                                   B

 THE FOUR SET OPERATORS
   UNION      -> all rows from both, duplicates REMOVED
   UNION ALL  -> all rows from both, duplicates KEPT
   EXCEPT     -> rows in the FIRST query that are NOT in the second (MINUS)
   INTERSECT  -> rows present in BOTH queries
*/


/*
===============================================================================
 2. SET RULES AND SYNTAX
===============================================================================

 SYNTAX
   SELECT col1, col2 FROM table1
   <SET OPERATOR>
   SELECT col1, col2 FROM table2
   ORDER BY col1;

 RULES (must follow, else you get an error)
 ------------------------------------------
 1. SAME NUMBER of columns in every SELECT.
 2. SAME ORDER of columns - SQL matches by POSITION, not by name.
 3. COMPATIBLE DATA TYPES in each position (INT with INT, text with text).
 4. Column NAMES come from the FIRST query (aliases of others are ignored).
 5. ORDER BY is written only ONCE, at the very END (it sorts the final result).
 6. WHERE / GROUP BY / HAVING belong to each individual SELECT.

 TIP: if a column is missing in one table, put NULL or a constant in its place
      so both queries have the same shape.
*/

-- Correct: same number, same order, same types
SELECT customer_id, first_name, 'CRM' AS source_system FROM crm_customers
UNION
SELECT customer_id, first_name, 'ERP' AS source_system FROM erp_customers;

-- Wrong: order of columns is different -> names get mixed up
-- SELECT first_name, customer_id FROM crm_customers
-- UNION
-- SELECT customer_id, first_name FROM erp_customers;   -- ERROR / wrong data

-- Filling a missing column with NULL
SELECT customer_id, first_name, phone FROM crm_customers
UNION ALL
SELECT customer_id, first_name, NULL  FROM erp_customers;


/*
===============================================================================
 3. UNION   -> all rows from both, DUPLICATES REMOVED
===============================================================================

 A ( ### ) B     -> everything, but each row appears only ONCE.

 SQL must sort and compare all rows to remove duplicates -> SLOWER.
*/

SELECT customer_id, first_name FROM crm_customers
UNION
SELECT customer_id, first_name FROM erp_customers;

-- If a customer exists in BOTH tables with the same values,
-- UNION shows that row only once.

-- USE WHEN: you need a unique master list (e.g. list of all customers).


/*
===============================================================================
 4. UNION ALL   -> all rows from both, DUPLICATES KEPT
===============================================================================

 Simply stacks the two results. No sorting, no comparison -> MUCH FASTER.
 This should be your DEFAULT choice.
*/

SELECT customer_id, first_name FROM crm_customers
UNION ALL
SELECT customer_id, first_name FROM erp_customers;

-- Trick: use UNION ALL to CHECK for duplicates
-- If UNION ALL count > UNION count, duplicates exist.
SELECT COUNT(*) FROM (
    SELECT customer_id FROM crm_customers
    UNION ALL
    SELECT customer_id FROM erp_customers
) AS t;

-- USE WHEN: you know there are no duplicates, or duplicates are wanted
--           (e.g. appending monthly files, loading data into a warehouse).


/*
===============================================================================
 5. EXCEPT   -> rows in the FIRST query that are NOT in the second
===============================================================================

 ( ###A### (     )  B  )
 Also called MINUS in Oracle. Duplicates are removed automatically.
 ORDER MATTERS: A EXCEPT B is NOT the same as B EXCEPT A.
*/

-- Customers that exist in CRM but not in ERP
SELECT customer_id, first_name FROM crm_customers
EXCEPT
SELECT customer_id, first_name FROM erp_customers;

-- Reverse direction gives a different answer
SELECT customer_id, first_name FROM erp_customers
EXCEPT
SELECT customer_id, first_name FROM crm_customers;

-- USE WHEN: find missing records, compare two systems, data quality checks.


/*
===============================================================================
 6. INTERSECT   -> rows present in BOTH queries
===============================================================================

 (  A  ( ### )  B  )
 Duplicates are removed automatically. Order does NOT matter.
*/

SELECT customer_id, first_name FROM crm_customers
INTERSECT
SELECT customer_id, first_name FROM erp_customers;

-- USE WHEN: find common records that match perfectly in both systems.


/*
===============================================================================
 7. USE CASE : COMBINE INFORMATION FROM DIFFERENT SOURCES
===============================================================================

 Very common in a Data Warehouse: the same kind of data comes from
 several source systems or several tables, and we need ONE table.
*/

-- Combine sales from two systems and mark where each row came from
SELECT order_id,
       customer_id,
       sales_amount,
       'CRM' AS source_system
FROM   crm_sales
UNION ALL
SELECT order_id,
       customer_id,
       sales_amount,
       'ERP' AS source_system
FROM   erp_sales;

-- Combine current year and archive (history) tables
SELECT * FROM sales_2024
UNION ALL
SELECT * FROM sales_archive;

-- Combine and then aggregate the whole thing
SELECT   source_system,
         COUNT(*)          AS total_orders,
         SUM(sales_amount) AS total_sales
FROM (
    SELECT order_id, sales_amount, 'CRM' AS source_system FROM crm_sales
    UNION ALL
    SELECT order_id, sales_amount, 'ERP' AS source_system FROM erp_sales
) AS all_sales
GROUP BY source_system;

-- TIP: always add a 'source_system' column, it makes debugging much easier.


/*
===============================================================================
 8. USE CASE : DELTA DETECTION  (what changed between two tables)
===============================================================================

 DELTA = the DIFFERENCE between two datasets.
 Used to compare SOURCE vs TARGET during an ETL load, or yesterday vs today.
*/

-- 1) NEW rows : in source but not in target  -> need INSERT
SELECT customer_id, first_name, country FROM source_customers
EXCEPT
SELECT customer_id, first_name, country FROM target_customers;

-- 2) DELETED rows : in target but not in source -> need DELETE
SELECT customer_id, first_name, country FROM target_customers
EXCEPT
SELECT customer_id, first_name, country FROM source_customers;

-- 3) UNCHANGED rows : identical in both -> do nothing
SELECT customer_id, first_name, country FROM source_customers
INTERSECT
SELECT customer_id, first_name, country FROM target_customers;

-- 4) FULL VALIDATION : if both EXCEPT queries return 0 rows,
--    the two tables are exactly the same.
SELECT 'Missing in target' AS issue, * FROM (
    SELECT * FROM source_customers EXCEPT SELECT * FROM target_customers
) AS a
UNION ALL
SELECT 'Missing in source' AS issue, * FROM (
    SELECT * FROM target_customers EXCEPT SELECT * FROM source_customers
) AS b;

-- NOTE: EXCEPT compares ALL selected columns, so a change in ANY column
--       makes the row appear as "different". That is exactly what we want
--       for delta detection.


/*
===============================================================================
 9. SET OPERATORS - EXTRA NOTES
===============================================================================

 PRIORITY when mixing operators
   INTERSECT runs FIRST, then UNION and EXCEPT (left to right).
   Use brackets () to control the order and to keep the query readable.
*/

(SELECT customer_id FROM crm_customers
 UNION
 SELECT customer_id FROM erp_customers)
EXCEPT
SELECT customer_id FROM blocked_customers;

-- ORDER BY only at the end, using the FIRST query's column names
SELECT customer_id, first_name FROM crm_customers
UNION ALL
SELECT customer_id, first_name FROM erp_customers
ORDER BY first_name;

-- NULL handling: set operators treat two NULLs as EQUAL (unlike = NULL).
-- So a NULL row in both tables is seen as a duplicate by UNION / INTERSECT.


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Operator    | Result                          | Duplicates | Speed | Order matters
 ------------|---------------------------------|------------|-------|--------------
 UNION       | A + B                           | removed    | slow  | no
 UNION ALL   | A + B                           | kept       | FAST  | no
 EXCEPT      | in A, not in B                  | removed    | slow  | YES
 INTERSECT   | in A and in B                   | removed    | slow  | no

 Remember
 - Same number of columns, same order, compatible types
 - Column names come from the FIRST query
 - One ORDER BY, at the very end
 - Prefer UNION ALL unless you really need duplicates removed
 - EXCEPT / INTERSECT are the easiest tools for comparing two tables
===============================================================================
*/
