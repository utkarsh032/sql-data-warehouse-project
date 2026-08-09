/*==== TOPIC : SQL - SELECT Statement ====*/

/*
===============================================================================
 1. COMPONENTS OF SQL
===============================================================================

 Every SQL statement is built from these parts:

 A) KEYWORDS / CLAUSES  -> reserved words that give the command
                           SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY

 B) IDENTIFIERS         -> names of database objects
                           table name, column name, schema name, alias
                           Example: gold.dim_customers, customer_name

 C) OPERATORS           -> used to compare or combine values
    1. Comparison  ->  =  <>  !=  >  <  >=  <=
    2. Logical     ->  AND  OR  NOT
    3. Range/Set   ->  BETWEEN  IN  LIKE  IS NULL
    4. Arithmetic  ->  +  -  *  /  %

 D) FUNCTIONS           -> ready made calculations
    1. Aggregate  -> COUNT(), SUM(), AVG(), MIN(), MAX()   (many rows -> 1 value)
    2. Scalar     -> UPPER(), LEN(), ROUND(), GETDATE()    (1 row -> 1 value)

 E) LITERALS / VALUES   -> fixed values written by us
                           'India'  (string)   100  (number)   '2024-01-01' (date)

 F) EXPRESSIONS         -> combination of columns, operators and functions
                           Example: price * quantity AS total_amount

 G) COMMENTS            -> notes for humans, ignored by SQL engine
                           -- single line
                           /* multi line */

 H) SEMICOLON ( ; )     -> marks the end of one statement
*/


/*
===============================================================================
 2. WHAT IS A SQL QUERY
===============================================================================

 A QUERY is a request sent to the database to get or change data.
 Most of the time "query" means a SELECT statement (reading data).

 INPUT  : Table(s) in the database
 OUTPUT : A RESULT SET (a temporary table of rows and columns)

 FULL STRUCTURE OF A SELECT QUERY (order we WRITE it):
 -----------------------------------------------------
   SELECT   DISTINCT / TOP n   column_list     -- what to show
   FROM     table_name                         -- from where
   WHERE    row_condition                      -- filter rows
   GROUP BY column_list                        -- make groups
   HAVING   group_condition                    -- filter groups
   ORDER BY column_list                        -- sort result

 IMPORTANT - LOGICAL EXECUTION ORDER (order SQL actually RUNS it):
 -----------------------------------------------------------------
   1. FROM      -> pick the table
   2. WHERE     -> filter rows            (cannot use alias, no aggregates)
   3. GROUP BY  -> make groups
   4. HAVING    -> filter groups          (aggregates allowed)
   5. SELECT    -> pick columns, create alias
   6. DISTINCT  -> remove duplicate rows
   7. ORDER BY  -> sort                   (alias CAN be used here)
   8. TOP       -> limit rows at the end

 This is WHY an alias made in SELECT works in ORDER BY but NOT in WHERE.
*/

-- Basic query using all main clauses
SELECT   country,
         COUNT(*) AS total_customers
FROM     customers
WHERE    country IS NOT NULL
GROUP BY country
HAVING   COUNT(*) > 1
ORDER BY total_customers DESC;


/*
===============================================================================
 3. SELECT & FROM
===============================================================================

 SELECT -> which COLUMNS to return
 FROM   -> which TABLE to read from

 Syntax:  SELECT column1, column2 FROM schema.table_name;
*/

-- Select all columns  (* = all). Avoid in production, it is slow.
SELECT * FROM customers;

-- Select specific columns (recommended)
SELECT customer_id, customer_name, country
FROM customers;

-- ALIAS : rename a column or table only in the output (AS is optional)
SELECT customer_name AS name,
       country       AS cust_country
FROM customers AS c;

-- EXPRESSION : calculate a new column
SELECT product_name,
       price,
       quantity,
       price * quantity AS total_amount
FROM orders;

-- Constant / literal column
SELECT customer_name,
       'Active' AS status
FROM customers;

-- Schema qualified name (schema.table) - used in this warehouse project
SELECT customer_key, first_name, country
FROM gold.dim_customers;


/*
===============================================================================
 4. WHERE  (filter ROWS before grouping)
===============================================================================

 Runs BEFORE GROUP BY, so aggregate functions are NOT allowed here.
 Column alias made in SELECT is also NOT allowed here.
*/

-- Simple condition
SELECT * FROM customers
WHERE country = 'India';

-- Comparison operators : =  <>  >  <  >=  <=
SELECT * FROM orders
WHERE price > 500;

-- AND -> both conditions must be true
SELECT * FROM customers
WHERE country = 'India' AND score > 500;

-- OR -> any one condition true
SELECT * FROM customers
WHERE country = 'India' OR country = 'USA';

-- NOT -> reverse the condition
SELECT * FROM customers
WHERE NOT country = 'India';

-- BETWEEN -> range check (both limits included)
SELECT * FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31';

-- IN -> match any value from a list (short form of many ORs)
SELECT * FROM customers
WHERE country IN ('India', 'USA', 'UK');

-- LIKE -> pattern search
--   %  = any number of characters
--   _  = exactly one character
SELECT * FROM customers
WHERE customer_name LIKE 'A%';      -- starts with A
-- LIKE '%a'    -> ends with a
-- LIKE '%ar%'  -> contains ar
-- LIKE '_r%'   -> r is the 2nd character

-- NULL check -> NULL means "unknown", use IS NULL / IS NOT NULL (never = NULL)
SELECT * FROM customers
WHERE country IS NULL;


/*
===============================================================================
 5. ORDER BY  (sort the result)
===============================================================================

 ASC  -> ascending  (A-Z, 0-9)  = DEFAULT
 DESC -> descending (Z-A, 9-0)

 Runs almost LAST, so column ALIAS is allowed here.
*/

-- Ascending (default)
SELECT customer_name, score
FROM customers
ORDER BY score;

-- Descending
SELECT customer_name, score
FROM customers
ORDER BY score DESC;

-- Sort by more than one column
-- First sort by country (A-Z), then inside each country sort score high to low
SELECT customer_name, country, score
FROM customers
ORDER BY country ASC, score DESC;

-- Sort by an alias (allowed, because ORDER BY runs after SELECT)
SELECT customer_name,
       price * quantity AS total_amount
FROM orders
ORDER BY total_amount DESC;


/*
===============================================================================
 6. GROUP BY  (combine rows into groups)
===============================================================================

 Used with AGGREGATE functions to get a summary per group.
   COUNT() -> number of rows
   SUM()   -> total
   AVG()   -> average
   MIN()   -> smallest
   MAX()   -> largest

 RULE: every column in SELECT must be either
       (a) inside an aggregate function, OR
       (b) listed in GROUP BY
*/

-- Total customers per country
SELECT   country,
         COUNT(*) AS total_customers
FROM     customers
GROUP BY country;

-- Multiple aggregates together
SELECT   country,
         COUNT(*)   AS total_customers,
         SUM(score) AS total_score,
         AVG(score) AS avg_score,
         MAX(score) AS highest_score
FROM     customers
GROUP BY country;

-- Group by more than one column (one group per country + status combination)
SELECT   country,
         status,
         COUNT(*) AS total
FROM     customers
GROUP BY country, status;

-- NOTE: COUNT(*) counts all rows including NULLs,
--       COUNT(column) skips NULL values in that column.


/*
===============================================================================
 7. HAVING  (filter GROUPS after grouping)
===============================================================================

 WHERE  -> filters ROWS   (before GROUP BY, no aggregates)
 HAVING -> filters GROUPS (after  GROUP BY, aggregates allowed)
*/

-- Only show countries having more than 2 customers
SELECT   country,
         COUNT(*) AS total_customers
FROM     customers
GROUP BY country
HAVING   COUNT(*) > 2;

-- WHERE and HAVING used together
-- 1. WHERE removes rows with no score
-- 2. GROUP BY makes country groups
-- 3. HAVING keeps only groups with average score above 500
SELECT   country,
         AVG(score) AS avg_score
FROM     customers
WHERE    score IS NOT NULL
GROUP BY country
HAVING   AVG(score) > 500
ORDER BY avg_score DESC;


/*
===============================================================================
 8. DISTINCT  (remove duplicate rows)
===============================================================================

 Applies to the WHOLE selected row, not to a single column.
 It is slow on big tables, use only when needed.
*/

-- List of unique countries
SELECT DISTINCT country
FROM customers;

-- Unique combination of country + status
SELECT DISTINCT country, status
FROM customers;

-- Count how many unique countries exist
SELECT COUNT(DISTINCT country) AS unique_countries
FROM customers;


/*
===============================================================================
 9. TOP  (limit number of rows)  -- SQL Server / T-SQL
===============================================================================

 TOP n         -> first n rows
 TOP n PERCENT -> first n percent of rows
 WITH TIES     -> also include rows that tie with the last row

 Other databases use LIMIT (MySQL, PostgreSQL)
 or FETCH FIRST n ROWS ONLY (standard SQL).
*/

-- First 5 rows (order is not guaranteed without ORDER BY)
SELECT TOP 5 * FROM customers;

-- Top 3 customers by score (always use ORDER BY with TOP)
SELECT TOP 3 customer_name, score
FROM customers
ORDER BY score DESC;

-- Lowest 3 scores
SELECT TOP 3 customer_name, score
FROM customers
ORDER BY score ASC;

-- Top 10 percent of rows
SELECT TOP 10 PERCENT customer_name, score
FROM customers
ORDER BY score DESC;

-- WITH TIES : if 4th row has the same score as the 3rd, it is also shown
SELECT TOP 3 WITH TIES customer_name, score
FROM customers
ORDER BY score DESC;

-- Paging alternative (skip 10 rows, take next 10)
SELECT customer_name, score
FROM customers
ORDER BY score DESC
OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY;


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Clause      | Work                          | Runs (order)
 ------------|-------------------------------|--------------
 FROM        | choose table                  | 1
 WHERE       | filter rows                   | 2
 GROUP BY    | make groups                   | 3
 HAVING      | filter groups                 | 4
 SELECT      | choose columns / alias        | 5
 DISTINCT    | remove duplicates             | 6
 ORDER BY    | sort result                   | 7
 TOP         | limit rows                    | 8

 WHERE vs HAVING -> rows vs groups
 ORDER BY        -> can use alias | WHERE -> cannot use alias
 COUNT(*)        -> counts NULLs  | COUNT(col) -> skips NULLs
 IS NULL         -> correct       | = NULL     -> wrong
===============================================================================
*/
