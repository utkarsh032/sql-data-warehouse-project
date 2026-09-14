/*==== TOPIC : SQL - NULL FUNCTIONS ====*/

/*
===============================================================================
 1. INTRO TO NULLS
===============================================================================

 NULL means "NO VALUE" / "UNKNOWN" / "MISSING".

 NULL IS NOT
   - zero  (0 is a known value)
   - empty string ''  (that is a known value: text of length 0)
   - the word 'NULL'  (that is text)

 WHY NULLS APPEAR
 - The value was never entered  (customer did not give a phone number)
 - An OUTER JOIN found no matching row
 - A calculation could not be done
 - The source file had a blank column

 THE GOLDEN RULE OF NULL
 -----------------------
 Any operation with NULL gives NULL, and any comparison gives UNKNOWN.

   NULL + 100        -> NULL
   NULL = NULL       -> UNKNOWN  (not TRUE!)
   'abc' + NULL      -> NULL
   WHERE col = NULL  -> returns NO rows

 That is why we need special functions to handle NULL.

 THE NULL FUNCTIONS
   ISNULL(a, b)     -> replace NULL with b            (SQL Server only)
   COALESCE(a,b,c)  -> first value that is not NULL   (standard SQL)
   NULLIF(a, b)     -> NULL if a = b, else a
   IS NULL / IS NOT NULL -> check for NULL
*/

-- Wrong: never use = NULL
SELECT * FROM customers WHERE country = NULL;      -- returns nothing

-- Correct
SELECT * FROM customers WHERE country IS NULL;


/*
===============================================================================
 2. COALESCE vs ISNULL   -> replace NULL with a default value
===============================================================================
*/

-- ISNULL : takes exactly 2 arguments
SELECT customer_name,
       ISNULL(country, 'Unknown') AS country
FROM   customers;

-- COALESCE : takes MANY arguments, returns the first non NULL one
SELECT customer_name,
       COALESCE(mobile, landline, email, 'No contact') AS contact
FROM   customers;

/*
 DIFFERENCES
 -------------------------------------------------------------------------
 Point            | ISNULL                  | COALESCE
 -----------------|-------------------------|---------------------------
 Standard SQL     | No (SQL Server only)    | Yes (works everywhere)
 Arguments        | exactly 2               | 2 or more
 Data type of     | type of the FIRST       | the highest priority type
 the result       | argument                | among all arguments
 CASE expression  | no                      | yes (it is CASE internally)
 Speed            | slightly faster         | slightly slower
 -------------------------------------------------------------------------

 RECOMMENDATION: use COALESCE. It is standard and more flexible.

 DATA TYPE TRAP with ISNULL
*/
SELECT ISNULL(CAST('abc' AS VARCHAR(3)), 'default');   -- result cut to 3 chars: 'def'
SELECT COALESCE(CAST('abc' AS VARCHAR(3)), 'default'); -- result: 'default'


/*
===============================================================================
 3. HANDLING NULL : DATA AGGREGATION
===============================================================================

 AGGREGATE FUNCTIONS IGNORE NULLS  (except COUNT(*)).
 This is usually helpful, but it can also hide problems.
*/

-- Example data: score = 100, 200, NULL, 400

SELECT COUNT(*)     AS count_all,     -- 4  (counts every ROW, NULLs included)
       COUNT(score) AS count_score,   -- 3  (skips the NULL)
       SUM(score)   AS total,         -- 700
       AVG(score)   AS average        -- 233  = 700 / 3   <- NULL is ignored!
FROM   customers;

-- If you want NULL to count as ZERO, replace it first
SELECT AVG(COALESCE(score, 0)) AS average_with_zero   -- 175 = 700 / 4
FROM   customers;

-- Which AVG is correct depends on the business question:
--   "average score of customers WHO HAVE a score"  -> AVG(score)
--   "average score of ALL customers"               -> AVG(COALESCE(score,0))

-- Count how many NULLs a column has (data quality check)
SELECT COUNT(*) - COUNT(country) AS missing_countries
FROM   customers;

-- Count NULL and NOT NULL in one query
SELECT SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_count,
       SUM(CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END) AS filled_count
FROM   customers;

-- GROUP BY puts all NULLs into ONE group
SELECT   COALESCE(country, 'Unknown') AS country,
         COUNT(*) AS total
FROM     customers
GROUP BY COALESCE(country, 'Unknown');


/*
===============================================================================
 4. HANDLING NULL : MATHEMATICAL OPERATIONS
===============================================================================

 In maths, one NULL destroys the whole calculation.
*/

-- Problem: if discount is NULL, the final price becomes NULL
SELECT product_name,
       price,
       discount,
       price - discount AS final_price      -- NULL when discount is NULL
FROM   products;

-- Solution: treat a missing discount as 0
SELECT product_name,
       price,
       COALESCE(discount, 0) AS discount,
       price - COALESCE(discount, 0) AS final_price
FROM   products;

-- Same problem with string concatenation using +
SELECT first_name + ' ' + last_name                       AS bad_name,  -- NULL
       first_name + ' ' + COALESCE(last_name, '')         AS good_name,
       CONCAT(first_name, ' ', last_name)                 AS best_name; -- CONCAT ignores NULL

-- Divide by zero protection using NULLIF (see section 6)
SELECT sales,
       quantity,
       sales / NULLIF(quantity, 0) AS price_per_unit
FROM   orders;


/*
===============================================================================
 5. HANDLING NULL : SORTING DATA
===============================================================================

 In SQL Server, NULL is treated as the SMALLEST value.
   ORDER BY col ASC   -> NULLs come FIRST
   ORDER BY col DESC  -> NULLs come LAST
*/

SELECT customer_name, score FROM customers ORDER BY score ASC;   -- NULLs on top
SELECT customer_name, score FROM customers ORDER BY score DESC;  -- NULLs at bottom

-- Force NULLs to the END even in ascending order
SELECT customer_name, score
FROM   customers
ORDER BY CASE WHEN score IS NULL THEN 1 ELSE 0 END,   -- 0 first, 1 last
         score ASC;

-- Force NULLs to the TOP in descending order
SELECT customer_name, score
FROM   customers
ORDER BY CASE WHEN score IS NULL THEN 0 ELSE 1 END,
         score DESC;

-- Simple trick: sort NULLs as if they were 0
SELECT customer_name, score
FROM   customers
ORDER BY COALESCE(score, 0) DESC;


/*
===============================================================================
 6. NULLIF   -> turn a value INTO a NULL
===============================================================================

 Syntax: NULLIF(a, b)
   if a = b  -> returns NULL
   else      -> returns a

 It is the OPPOSITE of COALESCE:
   COALESCE removes NULLs, NULLIF creates them.
*/

SELECT NULLIF(10, 10) AS same,        -- NULL
       NULLIF(10, 20) AS different;   -- 10

-- MAIN USE CASE : avoid the "divide by zero" error
SELECT sales,
       quantity,
       sales / NULLIF(quantity, 0) AS price_per_unit   -- NULL instead of error
FROM   orders;

-- Combine with COALESCE to show 0 instead of NULL
SELECT COALESCE(sales / NULLIF(quantity, 0), 0) AS price_per_unit
FROM   orders;

-- Turn placeholder values into real NULLs while cleaning data
SELECT NULLIF(country, 'n/a')       AS country,
       NULLIF(TRIM(first_name), '') AS first_name,
       NULLIF(phone, 'unknown')     AS phone
FROM   bronze.raw_customers;


/*
===============================================================================
 7. IS NULL & IS NOT NULL   -> checking for NULL
===============================================================================

 The ONLY correct way to test for NULL.
*/

SELECT * FROM customers WHERE country IS NULL;
SELECT * FROM customers WHERE country IS NOT NULL;

-- Combine with other conditions
SELECT * FROM customers
WHERE  country IS NOT NULL
  AND  score > 500;

-- Find rows where ANY important column is missing
SELECT * FROM customers
WHERE  first_name IS NULL
    OR country    IS NULL
    OR score      IS NULL;

-- LEFT ANTI JOIN uses IS NULL to find records with no match
SELECT c.customer_id, c.customer_name
FROM   customers c
LEFT   JOIN orders o ON c.customer_id = o.customer_id
WHERE  o.customer_id IS NULL;

-- WARNING: NOT IN with a NULL in the list returns ZERO rows
SELECT * FROM customers
WHERE  customer_id NOT IN (SELECT customer_id FROM orders);   -- risky

SELECT * FROM customers
WHERE  customer_id NOT IN (SELECT customer_id FROM orders
                           WHERE customer_id IS NOT NULL);    -- safe


/*
===============================================================================
 8. NULL vs EMPTY vs BLANK
===============================================================================

 Three values that LOOK the same on screen but are completely different:

   NULL   -> no value at all         (unknown)
   ''     -> EMPTY string            (a known value, length 0)
   '   '  -> BLANK string / spaces   (a known value, length > 0)
*/

-- See the difference clearly
SELECT 'NULL'  AS type, NULL AS value, LEN(NULL)  AS length, DATALENGTH(NULL)  AS bytes
UNION ALL
SELECT 'Empty', '',   LEN('')    , DATALENGTH('')
UNION ALL
SELECT 'Blank', '   ', LEN('   '), DATALENGTH('   ');

-- Result
--  type  | value | length | bytes
--  NULL  | NULL  | NULL   | NULL
--  Empty |       | 0      | 0
--  Blank |       | 0      | 3      <- LEN ignores trailing spaces

-- Find all three problem types in one query
SELECT customer_id, first_name,
       CASE WHEN first_name IS NULL        THEN 'NULL'
            WHEN first_name = ''           THEN 'Empty'
            WHEN TRIM(first_name) = ''     THEN 'Blank'
            ELSE 'OK'
       END AS value_type
FROM   customers;

-- Clean all three into a single consistent NULL
SELECT NULLIF(TRIM(first_name), '') AS first_name
FROM   bronze.raw_customers;
-- TRIM removes the spaces, NULLIF turns the empty result into NULL.


/*
===============================================================================
 9. HANDLING NULL : DATA POLICIES
===============================================================================

 A DATA POLICY is the RULE your team agrees on for handling missing values.
 Decide it ONCE and apply it everywhere, so all reports match.

 COMMON POLICIES
 ---------------
 1. KEEP THE NULL
    Best when "unknown" is meaningful and you must not invent data.
    Example: birthdate not provided.

 2. REPLACE WITH A DEFAULT
    Text    -> 'Unknown' / 'n/a'
    Number  -> 0
    Date    -> a far past / far future date
    Example: COALESCE(country, 'Unknown')

 3. REJECT THE ROW
    Do not load rows where a key column is missing.
    Example: an order with no customer_id.

 4. USE A DEFAULT CONSTRAINT so NULL never enters the table
    Example: country VARCHAR(50) DEFAULT 'Unknown'

 5. FORBID NULL for critical columns
    Example: customer_id INT NOT NULL

 PRACTICAL RULES USED IN A DATA WAREHOUSE
 - BRONZE layer : keep the data exactly as it came, NULLs included.
 - SILVER layer : clean and standardise ('' and '   ' become NULL,
                  codes become readable text, defaults applied).
 - GOLD layer   : no surprises for the business user, replace NULLs with
                  clear labels like 'Unknown' or 'n/a'.
 - KEY columns  : never allow NULL, always NOT NULL.
 - MEASURES     : replace NULL with 0 so sums and charts work.
 - Unmatched dimension rows: use a surrogate key of 0 pointing to an
   "Unknown" member instead of leaving NULL.
*/

-- Silver layer cleaning example
INSERT INTO silver.crm_cust_info (cst_id, cst_firstname, cst_gndr, cst_country)
SELECT cst_id,
       NULLIF(TRIM(cst_firstname), '')            AS cst_firstname,
       COALESCE(NULLIF(TRIM(cst_gndr), ''), 'n/a') AS cst_gndr,
       COALESCE(NULLIF(TRIM(cst_country), ''), 'Unknown') AS cst_country
FROM   bronze.crm_cust_info
WHERE  cst_id IS NOT NULL;      -- reject rows with no key

-- Gold layer: make sure measures never show NULL
SELECT p.product_name,
       COALESCE(SUM(f.sales_amount), 0) AS total_sales,
       COALESCE(SUM(f.quantity), 0)     AS total_quantity
FROM   gold.dim_products p
LEFT   JOIN gold.fact_sales f ON p.product_key = f.product_key
GROUP BY p.product_name;


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Function / Test        | Purpose                          | Example
 -----------------------|----------------------------------|------------------
 ISNULL(a, b)           | replace NULL (SQL Server, 2 args)| ISNULL(x,'n/a')
 COALESCE(a, b, c)      | first non NULL (standard)        | COALESCE(a,b,0)
 NULLIF(a, b)           | make NULL when a = b             | NULLIF(qty, 0)
 IS NULL                | test for missing value           | col IS NULL
 IS NOT NULL            | test for present value           | col IS NOT NULL

 Remember
 - NULL is unknown: NULL = NULL is NOT true
 - Never use = NULL, always IS NULL
 - Aggregates ignore NULL, COUNT(*) does not
 - One NULL in maths or in + concatenation makes the whole result NULL
 - NULLIF(x, 0) is the standard way to avoid divide by zero
 - NULL, '' and '   ' are three different things - clean them with
   NULLIF(TRIM(col), '')
 - Agree on a NULL policy and use it in every layer
===============================================================================
*/
