/*==== TOPIC : SQL - Filtering Data ====*/

/*
===============================================================================
 1. WHAT IS DATA FILTERING
===============================================================================

 Filtering = picking only the ROWS we need and ignoring the rest.
 In SQL it is done with the WHERE clause.

 Why filter?
 - Get only the data we care about (India customers, this year's orders)
 - Less data = FASTER query (the engine reads fewer rows)
 - Cleaner reports

 Where filtering happens:
   WHERE  -> filters ROWS   (before GROUP BY)  - no aggregate functions
   HAVING -> filters GROUPS (after  GROUP BY)  - aggregate functions allowed
   ON     -> filters while JOINING two tables

 Syntax:
   SELECT columns FROM table WHERE condition;

 A condition always gives TRUE / FALSE / UNKNOWN.
 Only rows where the condition is TRUE are returned.
*/

-- Sample data used in this file
--  customer_id | customer_name | country | score
--  1           | Utkarsh       | India   | 750
--  2           | John          | USA     | 900
--  3           | Maria         | Germany | 500
--  4           | Peter         | UK      | NULL


/*
===============================================================================
 2. COMPARISON OPERATORS
===============================================================================

   =    equal to
   <>   not equal to   (!= also works, <> is the standard)
   >    greater than
   <    less than
   >=   greater than or equal to
   <=   less than or equal to
*/

-- Equal
SELECT * FROM customers
WHERE country = 'India';

-- Not equal
SELECT * FROM customers
WHERE country <> 'India';

-- Greater than
SELECT * FROM customers
WHERE score > 500;

-- Less than or equal
SELECT * FROM customers
WHERE score <= 500;

-- Works on dates too (dates are written as text in quotes)
SELECT * FROM orders
WHERE order_date >= '2024-01-01';

-- NOTE: text comparison is usually CASE-INSENSITIVE in SQL Server
--       ('india' = 'India' is TRUE by default).


/*
===============================================================================
 3. AND OPERATOR   (all conditions must be TRUE)
===============================================================================
*/

-- Customers from India AND score above 500
SELECT * FROM customers
WHERE country = 'India'
  AND score   > 500;

-- More than two conditions
SELECT * FROM customers
WHERE country = 'India'
  AND score   > 500
  AND customer_name LIKE 'U%';

-- AND makes the result SMALLER (stricter filter).


/*
===============================================================================
 4. OR OPERATOR   (at least one condition must be TRUE)
===============================================================================
*/

-- Customers from India OR from USA
SELECT * FROM customers
WHERE country = 'India'
   OR country = 'USA';

-- OR makes the result BIGGER (looser filter).

-- ---- MIXING AND + OR : always use brackets () ----
-- AND is evaluated BEFORE OR, so without brackets the meaning changes.

-- WRONG (reads as: India, OR (USA and score>500))
SELECT * FROM customers
WHERE country = 'India' OR country = 'USA' AND score > 500;

-- CORRECT (India or USA) AND score > 500
SELECT * FROM customers
WHERE (country = 'India' OR country = 'USA')
  AND score > 500;

-- Operator priority:  NOT  >  AND  >  OR


/*
===============================================================================
 5. NOT OPERATOR   (reverses the condition)
===============================================================================
*/

-- Everyone who is NOT from India
SELECT * FROM customers
WHERE NOT country = 'India';

-- Same result using <>
SELECT * FROM customers
WHERE country <> 'India';

-- NOT with other operators
SELECT * FROM customers WHERE score NOT BETWEEN 100 AND 500;
SELECT * FROM customers WHERE country NOT IN ('India', 'USA');
SELECT * FROM customers WHERE customer_name NOT LIKE 'A%';
SELECT * FROM customers WHERE country IS NOT NULL;

-- CAREFUL WITH NULL:
-- NOT IN ('India', NULL) returns NO rows, because comparing with NULL
-- gives UNKNOWN. Always remove NULLs first.


/*
===============================================================================
 6. BETWEEN OPERATOR   (value inside a range)
===============================================================================

 BETWEEN low AND high  -> BOTH limits are INCLUDED (inclusive).
 Short form of:  column >= low AND column <= high
*/

-- Numbers
SELECT * FROM customers
WHERE score BETWEEN 500 AND 900;

-- Same thing written manually
SELECT * FROM customers
WHERE score >= 500 AND score <= 900;

-- Dates
SELECT * FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31';

-- NOT BETWEEN -> outside the range
SELECT * FROM customers
WHERE score NOT BETWEEN 500 AND 900;

-- RULE: low value must come FIRST.
--       BETWEEN 900 AND 500 returns nothing.

-- WARNING with DATETIME columns:
-- '2024-12-31' means '2024-12-31 00:00:00', so orders later that day are lost.
-- Safer way for datetime:
SELECT * FROM orders
WHERE order_date >= '2024-01-01'
  AND order_date <  '2025-01-01';


/*
===============================================================================
 7. IN OPERATOR   (value matches any item in a list)
===============================================================================

 Short and clean replacement for many OR conditions.
*/

-- Using IN
SELECT * FROM customers
WHERE country IN ('India', 'USA', 'UK');

-- Same thing using OR (longer)
SELECT * FROM customers
WHERE country = 'India' OR country = 'USA' OR country = 'UK';

-- NOT IN -> exclude a list
SELECT * FROM customers
WHERE country NOT IN ('India', 'USA');

-- IN with a SUBQUERY (list comes from another query)
SELECT * FROM customers
WHERE customer_id IN (SELECT customer_id FROM orders);

-- WARNING: NOT IN with NULL in the list gives ZERO rows.
-- Safe version:
SELECT * FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders
                          WHERE customer_id IS NOT NULL);


/*
===============================================================================
 8. LIKE OPERATOR   (pattern / text search)
===============================================================================

 WILDCARDS
   %      -> any number of characters (including zero)
   _      -> exactly ONE character
   [abc]  -> any one character from the set        (SQL Server)
   [a-f]  -> any one character in the range        (SQL Server)
   [^abc] -> any one character NOT in the set      (SQL Server)
*/

SELECT * FROM customers WHERE customer_name LIKE 'A%';    -- starts with A
SELECT * FROM customers WHERE customer_name LIKE '%n';    -- ends with n
SELECT * FROM customers WHERE customer_name LIKE '%ar%';  -- contains ar
SELECT * FROM customers WHERE customer_name LIKE '_a%';   -- 2nd letter is a
SELECT * FROM customers WHERE customer_name LIKE '__t%';  -- 3rd letter is t

-- Exactly 5 characters long
SELECT * FROM customers WHERE customer_name LIKE '_____';

-- Character sets (SQL Server only)
SELECT * FROM customers WHERE customer_name LIKE '[AJM]%';   -- starts A, J or M
SELECT * FROM customers WHERE customer_name LIKE '[^A]%';    -- does NOT start with A

-- NOT LIKE
SELECT * FROM customers WHERE customer_name NOT LIKE 'A%';

-- Searching for a real % or _ character -> use ESCAPE
SELECT * FROM products
WHERE product_name LIKE '%50!%%' ESCAPE '!';   -- finds "50%"

-- PERFORMANCE TIP:
-- LIKE 'A%'  -> can use an index      (FAST)
-- LIKE '%A'  -> cannot use an index   (SLOW, full table scan)


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Operator     | Meaning                    | Example
 -------------|----------------------------|-------------------------------
 = <> > < >= <=| compare values            | score > 500
 AND          | all conditions true        | a = 1 AND b = 2
 OR           | any condition true         | a = 1 OR  b = 2
 NOT          | reverse the condition      | NOT country = 'India'
 BETWEEN      | range, limits included     | score BETWEEN 100 AND 500
 IN           | match any value in a list  | country IN ('India','USA')
 LIKE         | text pattern               | name LIKE 'A%'
 IS NULL      | check for missing value    | country IS NULL

 Remember
 - Priority: NOT > AND > OR  -> use brackets when mixing AND with OR
 - BETWEEN includes both limits
 - NOT IN + NULL = no rows returned
 - Never use = NULL, always use IS NULL
===============================================================================
*/
