/*==== TOPIC : SQL - STRING FUNCTIONS ====*/

/*
===============================================================================
 1. WHAT IS DATA TRANSFORMATION
===============================================================================

 TRANSFORMATION = changing data from its raw form into a clean, useful form.
 It is the "T" in ETL (Extract - Transform - Load).

 Why we transform
 - Source data is dirty: extra spaces, wrong case, short codes, mixed formats
 - Reports need readable values ('M' -> 'Male')
 - Different systems must be made consistent before joining them

 Common transformations
   Cleaning   -> remove spaces, fix case, remove unwanted characters
   Combining  -> join first name + last name into full name
   Splitting  -> take the country code out of a customer key
   Mapping    -> convert codes into meaningful text
   Formatting -> change date / number display

 In this project this happens in the SILVER layer
 (bronze = raw data, silver = cleaned data, gold = business ready data).
*/


/*
===============================================================================
 2. SQL FUNCTIONS
===============================================================================

 A FUNCTION takes input, does something, and returns a value.
   Syntax:  FUNCTION_NAME(argument1, argument2)

 TWO MAIN GROUPS
 ---------------
 A) SCALAR FUNCTIONS   -> work on ONE row, return ONE value per row
    - String  : CONCAT, UPPER, LOWER, TRIM, REPLACE, LEN, LEFT, RIGHT, SUBSTRING
    - Number  : ROUND, ABS, CEILING, FLOOR
    - Date    : GETDATE, YEAR, MONTH, DATEADD, DATEDIFF
    - NULL    : ISNULL, COALESCE, NULLIF
    - Convert : CAST, CONVERT, FORMAT

 B) AGGREGATE FUNCTIONS -> work on MANY rows, return ONE value for the group
    - COUNT, SUM, AVG, MIN, MAX

 Functions can be NESTED (one inside another) - inner one runs first:
   UPPER(TRIM(first_name))
*/


/*
===============================================================================
 3. CONCAT   -> join two or more strings together
===============================================================================
*/

-- CONCAT function (recommended, it treats NULL as an empty string)
SELECT CONCAT(first_name, ' ', last_name) AS full_name
FROM   customers;

-- + operator (SQL Server) - but any NULL makes the WHOLE result NULL
SELECT first_name + ' ' + last_name AS full_name
FROM   customers;

-- Safe version with + : handle the NULL yourself
SELECT first_name + ' ' + ISNULL(last_name, '') AS full_name
FROM   customers;

-- CONCAT_WS : concat With Separator (separator written only once)
SELECT CONCAT_WS(', ', city, state, country) AS address
FROM   customers;

-- Build a readable label
SELECT CONCAT(customer_id, ' - ', first_name) AS customer_label
FROM   customers;


/*
===============================================================================
 4. UPPER & LOWER   -> change letter case
===============================================================================
*/

SELECT UPPER(first_name) AS upper_name,   -- UTKARSH
       LOWER(first_name) AS lower_name    -- utkarsh
FROM   customers;

-- Use for consistent comparison when the database IS case sensitive
SELECT * FROM customers
WHERE UPPER(country) = 'INDIA';

-- Standardise data during cleaning
UPDATE customers
SET    country = UPPER(TRIM(country));

-- Proper case (first letter capital) - SQL Server has no PROPER function,
-- so we build it manually
SELECT UPPER(LEFT(first_name, 1)) + LOWER(SUBSTRING(first_name, 2, LEN(first_name)))
       AS proper_name
FROM   customers;

-- WARNING: using a function on a column in WHERE stops the index from working
--          and makes the query slower.


/*
===============================================================================
 5. TRIM   -> remove spaces
===============================================================================

 Extra spaces are the most common problem in source data.
 'India ' and 'India' look the same but are NOT equal in SQL.
*/

SELECT TRIM(first_name)  AS clean_name;   -- removes spaces from both sides
SELECT LTRIM(first_name) AS left_trim;    -- removes spaces from the LEFT
SELECT RTRIM(first_name) AS right_trim;   -- removes spaces from the RIGHT

-- Find rows that have unwanted spaces (data quality check used in this project)
SELECT first_name
FROM   customers
WHERE  first_name <> TRIM(first_name);

-- Clean while loading into the silver layer
INSERT INTO silver.crm_cust_info (cst_firstname, cst_lastname)
SELECT TRIM(cst_firstname),
       TRIM(cst_lastname)
FROM   bronze.crm_cust_info;

-- TRIM can also remove other characters (SQL Server 2022+)
-- SELECT TRIM('.' FROM '...abc...');   -> abc


/*
===============================================================================
 6. REPLACE   -> replace one piece of text with another
===============================================================================

 Syntax: REPLACE(text, find_this, replace_with)
*/

-- Remove dashes from a phone number
SELECT REPLACE('123-456-789', '-', '') AS phone;      -- 123456789

-- Change a separator
SELECT REPLACE('2024/01/31', '/', '-') AS new_date;   -- 2024-01-31

-- Remove ALL spaces inside the text (TRIM only removes outer spaces)
SELECT REPLACE(first_name, ' ', '') AS no_spaces
FROM   customers;

-- Fix a wrong value in a column
UPDATE customers
SET    country = REPLACE(country, 'USA', 'United States');

-- Nested REPLACE to clean several characters at once
SELECT REPLACE(REPLACE(phone, '(', ''), ')', '') AS clean_phone
FROM   customers;


/*
===============================================================================
 7. LEN   -> length of a string (number of characters)
===============================================================================
*/

SELECT first_name,
       LEN(first_name) AS name_length
FROM   customers;

-- LEN ignores trailing spaces! 'abc   ' has LEN = 3
-- Use DATALENGTH to count bytes including trailing spaces
SELECT LEN('abc   ')        AS len_result,        -- 3
       DATALENGTH('abc   ') AS datalength_result; -- 6

-- Data quality: find codes that are not the expected length
SELECT * FROM customers
WHERE LEN(customer_code) <> 10;

-- Find empty or blank values
SELECT * FROM customers
WHERE LEN(TRIM(first_name)) = 0;


/*
===============================================================================
 8. LEFT & RIGHT   -> take characters from the start or the end
===============================================================================

 LEFT(text, n)  -> first n characters
 RIGHT(text, n) -> last  n characters
*/

SELECT LEFT('AW00011000', 3)  AS first_3;   -- AW0
SELECT RIGHT('AW00011000', 5) AS last_5;    -- 11000

-- Real example: split a product key into category id + product number
SELECT prd_key,
       LEFT(prd_key, 5)                        AS category_id,
       SUBSTRING(prd_key, 7, LEN(prd_key))     AS product_number
FROM   bronze.crm_prd_info;

-- Get the initial of a name
SELECT CONCAT(LEFT(first_name, 1), '.') AS initial
FROM   customers;

-- Get the year out of a text date
SELECT LEFT('2024-01-31', 4) AS year_part;   -- 2024


/*
===============================================================================
 9. SUBSTRING   -> take characters from the MIDDLE
===============================================================================

 Syntax: SUBSTRING(text, start_position, number_of_characters)
 IMPORTANT: counting starts at 1, not 0.
*/

SELECT SUBSTRING('Utkarsh', 2, 3) AS result;    -- tka

-- From position 3 to the end (use LEN as the length)
SELECT SUBSTRING(first_name, 3, LEN(first_name)) AS from_third
FROM   customers;

-- Remove the first character of every value
SELECT SUBSTRING(customer_code, 2, LEN(customer_code)) AS trimmed_code
FROM   customers;

-- Split an email into user name and domain using CHARINDEX
-- CHARINDEX(find, text) returns the position of a character
SELECT email,
       SUBSTRING(email, 1, CHARINDEX('@', email) - 1)              AS user_name,
       SUBSTRING(email, CHARINDEX('@', email) + 1, LEN(email))     AS domain
FROM   customers;

-- Other helpful string functions
--   CHARINDEX('@', email)   -> position of a character
--   REVERSE(text)           -> reverse the text
--   REPLICATE('x', 3)       -> xxx
--   STRING_AGG(name, ', ')  -> join many ROWS into one string
--   FORMAT(value, 'format') -> format numbers and dates as text


/*
===============================================================================
 10. NUMBER FUNCTIONS
===============================================================================
*/

-- ROUND(number, decimals) -> round to given decimal places
SELECT ROUND(3.14159, 2) AS result;    -- 3.14
SELECT ROUND(1234.56, -2) AS result;   -- 1200.00 (round to hundreds)

-- CEILING -> always round UP     | FLOOR -> always round DOWN
SELECT CEILING(3.2) AS up,             -- 4
       FLOOR(3.9)   AS down;           -- 3

-- ABS -> absolute value (removes the minus sign)
SELECT ABS(-25) AS result;             -- 25

-- POWER / SQRT
SELECT POWER(2, 3) AS cube,            -- 8
       SQRT(16)    AS square_root;     -- 4

-- Modulo % -> remainder of a division (useful for even/odd checks)
SELECT 10 % 3 AS remainder;            -- 1

-- INTEGER DIVISION TRAP: int / int gives an int
SELECT 10 / 3          AS wrong,       -- 3
       10.0 / 3        AS right_way,   -- 3.333333
       CAST(10 AS DECIMAL(10,2)) / 3 AS best;

-- Percentage calculation (cast first to avoid integer division)
SELECT product_name,
       ROUND(CAST(sales AS FLOAT) / SUM(sales) OVER () * 100, 2) AS pct_of_total
FROM   product_sales;

-- Aggregate number functions
SELECT COUNT(*)   AS total_rows,
       SUM(sales) AS total_sales,
       AVG(sales) AS avg_sales,
       MIN(sales) AS min_sales,
       MAX(sales) AS max_sales
FROM   orders;


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Function                  | Purpose                        | Example result
 --------------------------|--------------------------------|----------------
 CONCAT(a,' ',b)           | join strings                   | 'Utkarsh Raj'
 CONCAT_WS('-',a,b)        | join with a separator          | 'a-b'
 UPPER / LOWER             | change case                    | 'INDIA'/'india'
 TRIM / LTRIM / RTRIM      | remove spaces                  | 'India'
 REPLACE(t, old, new)      | swap text                      | '123456789'
 LEN(t)                    | number of characters           | 7
 LEFT(t,n) / RIGHT(t,n)    | first / last n characters      | 'AW0' / '11000'
 SUBSTRING(t,start,len)    | characters from the middle     | 'tka'
 CHARINDEX(c,t)            | position of a character        | 5
 ROUND / CEILING / FLOOR   | round numbers                  | 3.14 / 4 / 3
 ABS                       | remove minus sign              | 25

 Remember
 - CONCAT handles NULL, the + operator does not
 - SUBSTRING starts counting at 1
 - LEN ignores trailing spaces
 - int / int = int -> cast before dividing
 - Functions on columns in WHERE make queries slower (index is not used)
===============================================================================
*/
