/*==== TOPIC : SQL - CASE WHEN STATEMENT ====*/

/*
===============================================================================
 1. INTRO TO CASE STATEMENT
===============================================================================

 CASE adds IF-THEN-ELSE logic inside a SQL query.
 It checks conditions one by one and returns a value for the FIRST one
 that is TRUE.

 It is an EXPRESSION, not a statement: it returns ONE VALUE per row,
 so it can be used almost anywhere a column can be used
 (SELECT, WHERE, ORDER BY, GROUP BY, HAVING, UPDATE SET, aggregate functions).

 TWO FORMS
 ---------
 A) SEARCHED CASE  (flexible - use this one most of the time)
      CASE WHEN condition1 THEN result1
           WHEN condition2 THEN result2
           ELSE default_result
      END

 B) SIMPLE CASE  (short - only for equality checks on ONE column)
      CASE column
           WHEN value1 THEN result1
           WHEN value2 THEN result2
           ELSE default_result
      END
*/

-- Searched CASE
SELECT customer_name,
       score,
       CASE WHEN score >= 800 THEN 'High'
            WHEN score >= 500 THEN 'Medium'
            ELSE 'Low'
       END AS score_level
FROM   customers;

-- Simple CASE (same column compared to fixed values)
SELECT customer_id,
       gender_code,
       CASE gender_code
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE 'n/a'
       END AS gender
FROM   customers;

-- Simple CASE cannot use >, <, IS NULL, BETWEEN or AND/OR.
-- When in doubt, use the searched CASE.


/*
===============================================================================
 2. USE CASE : CATEGORIZING DATA  (bucketing / grouping ranges)
===============================================================================

 Turn a continuous number into readable groups (segments / buckets / bands).
*/

-- Customer segmentation by score
SELECT customer_name,
       score,
       CASE WHEN score >= 800 THEN 'Platinum'
            WHEN score >= 600 THEN 'Gold'
            WHEN score >= 400 THEN 'Silver'
            ELSE 'Bronze'
       END AS customer_tier
FROM   customers;

-- Age groups
SELECT customer_name,
       DATEDIFF(year, birthdate, GETDATE()) AS age,
       CASE WHEN DATEDIFF(year, birthdate, GETDATE()) < 20 THEN 'Under 20'
            WHEN DATEDIFF(year, birthdate, GETDATE()) < 30 THEN '20-29'
            WHEN DATEDIFF(year, birthdate, GETDATE()) < 50 THEN '30-49'
            ELSE '50 and above'
       END AS age_group
FROM   customers;

-- Categorize AND count in one report
SELECT   CASE WHEN sales_amount >= 1000 THEN 'High'
              WHEN sales_amount >= 500  THEN 'Medium'
              ELSE 'Low'
         END AS sales_band,
         COUNT(*)          AS total_orders,
         SUM(sales_amount) AS total_sales
FROM     orders
GROUP BY CASE WHEN sales_amount >= 1000 THEN 'High'
              WHEN sales_amount >= 500  THEN 'Medium'
              ELSE 'Low'
         END
ORDER BY total_sales DESC;

-- TIP: the same CASE must be repeated in GROUP BY because the SELECT alias
--      is not available there. A subquery or CTE avoids the repetition:
SELECT   sales_band,
         COUNT(*) AS total_orders
FROM (
    SELECT CASE WHEN sales_amount >= 1000 THEN 'High'
                WHEN sales_amount >= 500  THEN 'Medium'
                ELSE 'Low'
           END AS sales_band
    FROM   orders
) AS t
GROUP BY sales_band;


/*
===============================================================================
 3. CASE RULES
===============================================================================

 1. ORDER MATTERS
    Conditions are checked TOP to BOTTOM. The first TRUE one wins and the
    rest are skipped. Put the most specific / highest condition first.

 2. ELSE IS OPTIONAL BUT RECOMMENDED
    If no condition matches and there is no ELSE, the result is NULL.

 3. ALL RESULTS MUST HAVE COMPATIBLE DATA TYPES
    You cannot return 'High' in one branch and 100 in another.

 4. EVERY CASE MUST END WITH "END"

 5. IT RETURNS ONE VALUE PER ROW
    CASE cannot return a whole row or several columns.

 6. IT CAN BE NESTED (a CASE inside another CASE) - but keep it readable.
*/

-- Rule 1: wrong order - everything above 400 becomes 'Low'
SELECT CASE WHEN score >= 400 THEN 'Low'
            WHEN score >= 800 THEN 'High'    -- never reached!
       END AS wrong_order
FROM   customers;

-- Rule 2: no ELSE -> unmatched rows return NULL
SELECT CASE WHEN score >= 800 THEN 'High' END AS may_be_null
FROM   customers;

-- Rule 3: mixed data types cause an error / unwanted conversion
-- CASE WHEN score >= 800 THEN 'High' ELSE 0 END      -- avoid this

-- Rule 6: nested CASE
SELECT customer_name,
       CASE WHEN country = 'India'
            THEN CASE WHEN score >= 500 THEN 'India - Good'
                      ELSE 'India - Low' END
            ELSE 'Other country'
       END AS segment
FROM   customers;

-- CASE in other clauses
-- ORDER BY : custom sort order
SELECT customer_name, tier
FROM   customers
ORDER BY CASE tier WHEN 'Platinum' THEN 1
                   WHEN 'Gold'     THEN 2
                   WHEN 'Silver'   THEN 3
                   ELSE 4 END;

-- WHERE : conditional filter
SELECT * FROM orders
WHERE CASE WHEN status = 'Cancelled' THEN 0 ELSE 1 END = 1;

-- UPDATE : set different values in one statement
UPDATE customers
SET    tier = CASE WHEN score >= 800 THEN 'Platinum'
                   WHEN score >= 500 THEN 'Gold'
                   ELSE 'Silver' END;

-- Inside an aggregate : conditional aggregation (very powerful)
SELECT SUM(CASE WHEN country = 'India' THEN sales_amount ELSE 0 END) AS india_sales,
       SUM(CASE WHEN country = 'USA'   THEN sales_amount ELSE 0 END) AS usa_sales,
       COUNT(CASE WHEN status = 'Shipped' THEN 1 END)                AS shipped_orders
FROM   orders;
-- This is how you build a PIVOT style report without the PIVOT keyword.


/*
===============================================================================
 4. USE CASE : MAPPING VALUES  (codes -> readable text)
===============================================================================

 Source systems store short codes. Reports need full words.
 This is one of the main transformations in the SILVER layer.
*/

-- Map gender codes
SELECT cst_id,
       CASE UPPER(TRIM(cst_gndr))
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE 'n/a'
       END AS gender
FROM   bronze.crm_cust_info;

-- Map marital status codes
SELECT cst_id,
       CASE UPPER(TRIM(cst_marital_status))
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE 'n/a'
       END AS marital_status
FROM   bronze.crm_cust_info;

-- Map country codes (with several possible spellings)
SELECT CASE WHEN UPPER(TRIM(country)) IN ('US', 'USA', 'UNITED STATES')
            THEN 'United States'
            WHEN UPPER(TRIM(country)) IN ('DE', 'GERMANY')
            THEN 'Germany'
            WHEN TRIM(country) = '' OR country IS NULL
            THEN 'n/a'
            ELSE TRIM(country)
       END AS country
FROM   bronze.erp_loc_a101;

-- Map a status flag to text
SELECT order_id,
       CASE status_flag WHEN 1 THEN 'Active'
                        WHEN 0 THEN 'Inactive'
                        ELSE 'Unknown' END AS status
FROM   orders;

-- TIP: for a LONG list of codes, a mapping TABLE + JOIN is better than a
--      giant CASE, because the list can be updated without changing the code.


/*
===============================================================================
 5. USE CASE : HANDLING NULLS
===============================================================================

 CASE can do everything ISNULL / COALESCE / NULLIF do, and more,
 because it can also check other conditions at the same time.
*/

-- Replace NULL with a label (same as COALESCE)
SELECT customer_name,
       CASE WHEN country IS NULL THEN 'Unknown' ELSE country END AS country
FROM   customers;

-- Handle NULL, empty and blank in ONE expression
SELECT CASE WHEN first_name IS NULL          THEN 'Missing'
            WHEN TRIM(first_name) = ''       THEN 'Missing'
            ELSE TRIM(first_name)
       END AS first_name
FROM   bronze.raw_customers;

-- Avoid divide by zero (same as NULLIF, but you control the result)
SELECT sales,
       quantity,
       CASE WHEN quantity = 0 OR quantity IS NULL THEN 0
            ELSE sales / quantity
       END AS price_per_unit
FROM   orders;

-- Treat NULL as zero only inside a calculation
SELECT price - CASE WHEN discount IS NULL THEN 0 ELSE discount END AS final_price
FROM   products;

-- Report how complete the data is
SELECT   CASE WHEN country IS NULL THEN 'Missing' ELSE 'Provided' END AS data_status,
         COUNT(*) AS total_customers
FROM     customers
GROUP BY CASE WHEN country IS NULL THEN 'Missing' ELSE 'Provided' END;

-- Push NULLs to the end while sorting
SELECT customer_name, score
FROM   customers
ORDER BY CASE WHEN score IS NULL THEN 1 ELSE 0 END, score DESC;

/*
 WHICH ONE TO USE
   COALESCE -> simple "replace NULL with a default"        (shortest)
   NULLIF   -> simple "turn a value into NULL"
   CASE     -> anything more complex, several conditions, ranges, mapping
*/


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Form            | Syntax                                    | Use for
 ----------------|-------------------------------------------|-----------------
 Searched CASE   | CASE WHEN cond THEN val ... ELSE val END   | ranges, AND/OR,
                 |                                           | IS NULL, anything
 Simple CASE     | CASE col WHEN val THEN val ... END         | equality only

 MAIN USE CASES
   Categorizing  -> turn numbers into bands (High / Medium / Low)
   Mapping       -> turn codes into words ('M' -> 'Male')
   NULL handling -> replace or flag missing values
   Conditional aggregation -> SUM(CASE WHEN ... THEN x ELSE 0 END)
   Custom sorting -> ORDER BY CASE ...

 Remember
 - Conditions are checked top to bottom, first TRUE wins
 - Always finish with END
 - Add ELSE, otherwise unmatched rows become NULL
 - All THEN/ELSE results must be the same kind of data type
 - Repeat the CASE in GROUP BY, or wrap it in a subquery / CTE
===============================================================================
*/
