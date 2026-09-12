/*==== TOPIC : SQL - JOINS ====*/

/*
===============================================================================
 1. WHAT IS COMBINING DATA
===============================================================================

 Real data is stored in MANY tables (to avoid repeating data).
 To answer a business question we often need data from more than one table.
 Combining = bringing that data together into one result.

 TWO WAYS TO COMBINE DATA
 ------------------------
 A) JOIN            -> combine COLUMNS  (side by side, horizontally)
                       Tables are matched using a KEY column.
                       Result has MORE COLUMNS.

 B) SET OPERATOR    -> combine ROWS     (one below the other, vertically)
                       UNION, UNION ALL, EXCEPT, INTERSECT
                       Result has MORE ROWS.

      JOIN                          UNION
      A | B                          A
      --+--                          -
                                     B
*/


/*
===============================================================================
 2. INTRO TO JOINS
===============================================================================

 A JOIN matches rows of two tables using a common column (the KEY).

   customers                 orders
   -----------               ---------------------------
   customer_id  name         order_id  customer_id  amount
   1            Utkarsh      101       1            500
   2            John         102       1            300
   3            Maria        103       2            700
   4            Peter        104       5            900   <- no such customer

 KEY = customer_id (primary key in customers, foreign key in orders)

 SYNTAX
   SELECT columns
   FROM   table1 AS a
   <JOIN TYPE> table2 AS b
        ON a.key = b.key;

 - ON tells SQL HOW to match the rows.
 - Use table ALIASES (a, b / c, o) to keep the query short and clear.
 - Always write table.column when both tables have the same column name.

 TYPES OF JOIN
   INNER JOIN       -> only matching rows
   LEFT JOIN        -> all left rows  + matching right rows
   RIGHT JOIN       -> all right rows + matching left rows
   FULL JOIN        -> everything from both sides
   LEFT ANTI JOIN   -> only left rows with NO match
   RIGHT ANTI JOIN  -> only right rows with NO match
   FULL ANTI JOIN   -> only rows with NO match on either side
   CROSS JOIN       -> every row with every row (all combinations)
*/


/*
===============================================================================
 3. NO JOIN  (query the tables separately)
===============================================================================
 Use when the tables are not related, or you just want to look at them.
*/

SELECT * FROM customers;
SELECT * FROM orders;


/*
===============================================================================
 4. INNER JOIN   -> ONLY the matching rows
===============================================================================

  customers    orders
     (  A  ( ### )  B  )        ### = returned
 Rows with no match on either side are DROPPED.
*/

SELECT c.customer_id,
       c.customer_name,
       o.order_id,
       o.amount
FROM   customers AS c
INNER  JOIN orders AS o
       ON c.customer_id = o.customer_id;

-- 'INNER' is optional, plain JOIN means INNER JOIN
SELECT c.customer_name, o.amount
FROM   customers c
JOIN   orders o ON c.customer_id = o.customer_id;

-- USE WHEN: you want only customers who actually placed an order.


/*
===============================================================================
 5. LEFT JOIN   -> ALL rows from the LEFT table + matches from the right
===============================================================================

  ( ###A### ( ### )  B  )
 If there is no match, the right side columns become NULL.
*/

SELECT c.customer_id,
       c.customer_name,
       o.order_id,
       o.amount
FROM   customers AS c
LEFT   JOIN orders AS o
       ON c.customer_id = o.customer_id;

-- Result: every customer is shown.
-- Customers with no order get order_id = NULL, amount = NULL.

-- USE WHEN: you need the full list from the main table plus extra info.
-- This is the MOST USED join in reporting and data warehousing.


/*
===============================================================================
 6. RIGHT JOIN   -> ALL rows from the RIGHT table + matches from the left
===============================================================================

  (  A  ( ### ) ###B### )
*/

SELECT c.customer_name,
       o.order_id,
       o.amount
FROM   customers AS c
RIGHT  JOIN orders AS o
       ON c.customer_id = o.customer_id;

-- Result: every order is shown, even orders with an unknown customer.

-- TIP: RIGHT JOIN can always be rewritten as a LEFT JOIN by swapping tables.
--      Most developers only use LEFT JOIN because it is easier to read.
SELECT c.customer_name, o.order_id
FROM   orders AS o
LEFT   JOIN customers AS c
       ON c.customer_id = o.customer_id;


/*
===============================================================================
 7. FULL JOIN   -> everything from BOTH tables
===============================================================================

  ( ###A### ( ### ) ###B### )
 Matching rows are joined, non matching rows come with NULLs on the other side.
*/

SELECT c.customer_id,
       c.customer_name,
       o.order_id,
       o.amount
FROM   customers AS c
FULL   OUTER JOIN orders AS o
       ON c.customer_id = o.customer_id;

-- USE WHEN: comparing two systems and you want to see everything,
--           including what is missing on either side.
-- 'OUTER' is optional: FULL JOIN = FULL OUTER JOIN (same for LEFT/RIGHT).


/*
===============================================================================
 8. LEFT ANTI JOIN   -> left rows that have NO match on the right
===============================================================================

  ( ###A### (     )  B  )
 There is no LEFT ANTI JOIN keyword. We make it with LEFT JOIN + IS NULL.
*/

SELECT c.customer_id,
       c.customer_name
FROM   customers AS c
LEFT   JOIN orders AS o
       ON c.customer_id = o.customer_id
WHERE  o.customer_id IS NULL;

-- USE WHEN: find customers who NEVER placed an order.
--           find products that were never sold.


/*
===============================================================================
 9. RIGHT ANTI JOIN   -> right rows that have NO match on the left
===============================================================================

  (  A  (     ) ###B### )
*/

SELECT o.order_id,
       o.customer_id
FROM   customers AS c
RIGHT  JOIN orders AS o
       ON c.customer_id = o.customer_id
WHERE  c.customer_id IS NULL;

-- USE WHEN: find orphan records - orders whose customer does not exist.
--           very useful as a DATA QUALITY CHECK.


/*
===============================================================================
 10. FULL ANTI JOIN   -> rows that do NOT match on either side
===============================================================================

  ( ###A### (     ) ###B### )
*/

SELECT c.customer_id,
       c.customer_name,
       o.order_id
FROM   customers AS c
FULL   OUTER JOIN orders AS o
       ON c.customer_id = o.customer_id
WHERE  c.customer_id IS NULL
    OR o.customer_id IS NULL;

-- USE WHEN: find everything that does NOT match between two systems.


/*
===============================================================================
 11. CROSS JOIN   -> every row with every row (Cartesian product)
===============================================================================

 No ON condition. Result rows = rows(A) x rows(B).
 4 customers x 3 products = 12 rows.
*/

SELECT c.customer_name,
       p.product_name
FROM   customers AS c
CROSS  JOIN products AS p;

-- USE WHEN: build all possible combinations,
--           e.g. every product for every month (to fill missing months).

-- WARNING: a missing ON condition turns any join into an accidental CROSS JOIN
--          and can explode into millions of rows.


/*
===============================================================================
 12. HOW TO CHOOSE THE CORRECT JOIN
===============================================================================

 Ask: "Which rows must appear in my result?"

 Question                                       -> Join
 -----------------------------------------------------------------
 Only rows present in BOTH tables               -> INNER JOIN
 All rows of the MAIN table + extra info        -> LEFT JOIN
 All rows of the SECOND table + extra info      -> RIGHT JOIN (or swap + LEFT)
 Everything from both tables                    -> FULL JOIN
 Rows in A but NOT in B                         -> LEFT ANTI JOIN
 Rows in B but NOT in A                         -> RIGHT ANTI JOIN
 Rows that match nowhere                        -> FULL ANTI JOIN
 All possible combinations                      -> CROSS JOIN

 PRACTICAL TIPS
 - In a Data Warehouse: FACT table LEFT JOIN DIMENSION tables.
 - Join on KEY columns that are indexed, not on text descriptions.
 - Both key columns should have the SAME data type, else it is slow.
 - Duplicate keys in the right table MULTIPLY the rows -> check counts.

 WHERE vs ON in an OUTER JOIN (very common mistake)
 --------------------------------------------------
 ON    -> filter applied BEFORE the join, keeps all left rows
 WHERE -> filter applied AFTER  the join, turns LEFT JOIN into INNER JOIN
*/

-- Keeps all customers (filter is part of the join)
SELECT c.customer_name, o.amount
FROM   customers c
LEFT   JOIN orders o
       ON c.customer_id = o.customer_id
      AND o.amount > 500;

-- Drops customers without orders (behaves like an INNER JOIN)
SELECT c.customer_name, o.amount
FROM   customers c
LEFT   JOIN orders o
       ON c.customer_id = o.customer_id
WHERE  o.amount > 500;


/*
===============================================================================
 13. MULTI TABLE JOINS
===============================================================================

 We can join as many tables as we need. SQL joins them two at a time,
 from top to bottom.
*/

-- 3 table join
SELECT c.customer_name,
       o.order_id,
       p.product_name,
       o.amount
FROM   orders   AS o
INNER  JOIN customers AS c ON o.customer_id = c.customer_id
INNER  JOIN products  AS p ON o.product_id  = p.product_id;

-- Star schema example (this project) : one FACT + many DIMENSIONS
SELECT f.order_number,
       c.first_name,
       c.country,
       p.product_name,
       p.category,
       f.sales_amount,
       f.quantity
FROM   gold.fact_sales    AS f
LEFT   JOIN gold.dim_customers AS c ON f.customer_key = c.customer_key
LEFT   JOIN gold.dim_products  AS p ON f.product_key  = p.product_key;

-- Mixing join types + aggregation
SELECT   c.country,
         COUNT(DISTINCT c.customer_id) AS customers,
         COUNT(o.order_id)             AS orders,
         SUM(o.amount)                 AS total_sales
FROM     customers AS c
LEFT     JOIN orders AS o ON c.customer_id = o.customer_id
GROUP BY c.country
ORDER BY total_sales DESC;

/*
 RULES FOR MULTI TABLE JOINS
 - Start with the MAIN table (usually the fact table).
 - Once you use a LEFT JOIN, keep using LEFT JOIN for the next tables,
   otherwise a later INNER JOIN will remove the NULL rows you kept.
 - Give every table a short alias.
 - Build the query step by step: join 2 tables, check the row count, then add
   the next table.
*/


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Join type        | Returns
 -----------------|-------------------------------------------------
 INNER            | only matching rows
 LEFT             | all left + matching right (NULLs if no match)
 RIGHT            | all right + matching left
 FULL             | all rows from both sides
 LEFT ANTI        | left rows with no match      (LEFT  + IS NULL)
 RIGHT ANTI       | right rows with no match     (RIGHT + IS NULL)
 FULL ANTI        | rows matching nowhere        (FULL  + IS NULL)
 CROSS            | every combination (no ON)

 - JOIN adds COLUMNS, UNION adds ROWS
 - Filter in ON to keep outer rows, in WHERE to remove them
 - No ON condition = accidental CROSS JOIN
===============================================================================
*/
