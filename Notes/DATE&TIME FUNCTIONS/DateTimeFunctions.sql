/*==== TOPIC : SQL - DATE & TIME FUNCTIONS ====*/

/*
===============================================================================
 1. WHAT IS DATE AND TIME
===============================================================================

 Almost every business table has a date column:
 order_date, create_date, ship_date, birthdate.
 Date columns let us answer questions like
 "sales this month", "customers older than 30", "delivery delay in days".

 DATE / TIME DATA TYPES (SQL Server)
 -----------------------------------
   DATE            -> 2024-01-31                    (3 bytes)
   TIME            -> 14:30:00
   DATETIME2       -> 2024-01-31 14:30:00.1234567   (recommended)
   DATETIME        -> old type, less accurate
   SMALLDATETIME   -> old type, minute accuracy
   DATETIMEOFFSET  -> date + time + time zone

 STANDARD FORMAT
   Always write dates as 'YYYY-MM-DD' (ISO format).
   This is understood the same way on every server and language setting.
   '01/02/2024' is dangerous: it can mean 1 Feb or 2 Jan.

 CURRENT DATE AND TIME
   GETDATE()        -> server date + time
   SYSDATETIME()    -> more accurate version of GETDATE()
   GETUTCDATE()     -> UTC date + time
   CAST(GETDATE() AS DATE) -> today's date without time
*/

SELECT GETDATE()                  AS now,
       SYSDATETIME()              AS now_precise,
       CAST(GETDATE() AS DATE)    AS today,
       CAST(GETDATE() AS TIME)    AS current_time;


/*
===============================================================================
 2. DATE AND TIME FUNCTIONS - OVERVIEW
===============================================================================

 GROUP 1 - EXTRACT a part of a date
   DAY(), MONTH(), YEAR(), DATEPART(), DATENAME(), DATETRUNC(), EOMONTH()

 GROUP 2 - FORMAT / CAST (change the type or the look)
   FORMAT(), CONVERT(), CAST()

 GROUP 3 - CALCULATE with dates
   DATEADD(), DATEDIFF()

 GROUP 4 - VALIDATE
   ISDATE()
*/


/*
===============================================================================
 3. DAY, MONTH, YEAR   -> extract a number from a date
===============================================================================
*/

SELECT order_date,
       DAY(order_date)   AS day_number,     -- 31
       MONTH(order_date) AS month_number,   -- 1
       YEAR(order_date)  AS year_number     -- 2024
FROM   orders;

-- Filter by year and month
SELECT * FROM orders
WHERE  YEAR(order_date)  = 2024
  AND  MONTH(order_date) = 1;

-- Yearly sales report
SELECT   YEAR(order_date) AS order_year,
         SUM(sales_amount) AS total_sales
FROM     orders
GROUP BY YEAR(order_date)
ORDER BY order_year;

-- PERFORMANCE TIP: YEAR(order_date) = 2024 in WHERE blocks the index.
-- Faster version using a range:
SELECT * FROM orders
WHERE  order_date >= '2024-01-01'
  AND  order_date <  '2025-01-01';


/*
===============================================================================
 4. DATEPART   -> extract ANY part as a NUMBER
===============================================================================

 Syntax: DATEPART(part, date)

 Common parts:
   year, quarter, month, dayofyear, day, week, weekday, hour, minute, second
*/

SELECT order_date,
       DATEPART(year,    order_date) AS yr,
       DATEPART(quarter, order_date) AS qtr,      -- 1 to 4
       DATEPART(month,   order_date) AS mth,
       DATEPART(week,    order_date) AS wk,       -- week of the year
       DATEPART(weekday, order_date) AS wkday,    -- 1 = Sunday (default)
       DATEPART(hour,    order_date) AS hr
FROM   orders;

-- Quarterly sales
SELECT   DATEPART(quarter, order_date) AS quarter,
         SUM(sales_amount) AS total_sales
FROM     orders
GROUP BY DATEPART(quarter, order_date);

-- DATEPART returns a NUMBER. Use DATENAME when you need TEXT.


/*
===============================================================================
 5. DATENAME   -> extract a part as TEXT (name)
===============================================================================
*/

SELECT order_date,
       DATENAME(month,   order_date) AS month_name,   -- January
       DATENAME(weekday, order_date) AS day_name,     -- Wednesday
       DATENAME(year,    order_date) AS year_text     -- '2024'
FROM   orders;

-- Sales by day of the week (name for reading, number for sorting)
SELECT   DATENAME(weekday, order_date) AS day_name,
         SUM(sales_amount)             AS total_sales
FROM     orders
GROUP BY DATENAME(weekday, order_date), DATEPART(weekday, order_date)
ORDER BY DATEPART(weekday, order_date);

-- NOTE: the language of the name depends on the server language setting.


/*
===============================================================================
 6. DATETRUNC   -> cut the date down to a level  (SQL Server 2022+)
===============================================================================

 Keeps the part you ask for and resets everything smaller to its start value.
 Perfect for grouping by month / quarter / year.
*/

SELECT order_date,
       DATETRUNC(month,   order_date) AS month_start,   -- 2024-01-01
       DATETRUNC(quarter, order_date) AS quarter_start, -- 2024-01-01
       DATETRUNC(year,    order_date) AS year_start     -- 2024-01-01
FROM   orders;

-- Monthly trend, sorted correctly because the value is a real date
SELECT   DATETRUNC(month, order_date) AS order_month,
         SUM(sales_amount)            AS total_sales
FROM     orders
GROUP BY DATETRUNC(month, order_date)
ORDER BY order_month;

-- Older SQL Server versions: build the first day of the month manually
SELECT DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS month_start
FROM   orders;


/*
===============================================================================
 7. EOMONTH   -> End Of MONTH (last day of the month)
===============================================================================
*/

SELECT order_date,
       EOMONTH(order_date)     AS month_end,       -- 2024-01-31
       EOMONTH(order_date, 1)  AS next_month_end,  -- 2024-02-29
       EOMONTH(order_date, -1) AS prev_month_end   -- 2023-12-31
FROM   orders;

-- First day of the month using EOMONTH
SELECT DATEADD(day, 1, EOMONTH(order_date, -1)) AS month_start
FROM   orders;

-- Month end snapshot report
SELECT   EOMONTH(order_date)  AS month_end,
         SUM(sales_amount)    AS monthly_sales
FROM     orders
GROUP BY EOMONTH(order_date)
ORDER BY month_end;


/*
===============================================================================
 8. USE CASE : DATE EXTRACTION
===============================================================================
*/

-- Build a small date breakdown for reporting
SELECT order_id,
       order_date,
       YEAR(order_date)                 AS order_year,
       DATEPART(quarter, order_date)    AS order_quarter,
       MONTH(order_date)                AS order_month_no,
       DATENAME(month, order_date)      AS order_month_name,
       DATENAME(weekday, order_date)    AS order_day_name,
       DATETRUNC(month, order_date)     AS month_start,
       EOMONTH(order_date)              AS month_end
FROM   orders;

-- Age of a customer in years
SELECT customer_name,
       birthdate,
       DATEDIFF(year, birthdate, GETDATE()) AS age
FROM   customers;

-- Split orders into weekday and weekend
SELECT   CASE WHEN DATEPART(weekday, order_date) IN (1, 7)
              THEN 'Weekend' ELSE 'Weekday' END AS day_type,
         COUNT(*) AS total_orders
FROM     orders
GROUP BY CASE WHEN DATEPART(weekday, order_date) IN (1, 7)
              THEN 'Weekend' ELSE 'Weekday' END;


/*
===============================================================================
 9. COMPARE THE EXTRACT FUNCTIONS
===============================================================================

 Input date: 2024-01-31

 Function                        | Returns        | Data type
 --------------------------------|----------------|-----------
 YEAR(d)                         | 2024           | INT
 MONTH(d)                        | 1              | INT
 DAY(d)                          | 31             | INT
 DATEPART(month, d)              | 1              | INT
 DATENAME(month, d)              | 'January'      | VARCHAR
 DATETRUNC(month, d)             | 2024-01-01     | DATE
 EOMONTH(d)                      | 2024-01-31     | DATE
 FORMAT(d, 'MMM yyyy')           | 'Jan 2024'     | VARCHAR

 WHICH ONE TO USE
 - Need a number for maths or sorting     -> YEAR / MONTH / DATEPART
 - Need a readable label in a report      -> DATENAME / FORMAT
 - Need to GROUP BY month/quarter/year    -> DATETRUNC  (keeps sorting correct)
 - Need the last day of the month         -> EOMONTH

 WARNING: never GROUP BY a text month name alone - 'April' sorts before
 'January'. Group by a real date or by the month number.
*/


/*
===============================================================================
 10. INTRO TO FORMATTING & CASTING
===============================================================================

 FORMATTING -> change how a value LOOKS (date  -> text). For display only.
 CASTING    -> change the DATA TYPE of a value (text -> date, int -> decimal).

 Rule of thumb
 - Store dates as DATE / DATETIME2, never as text.
 - Format only at the very end, in the report layer.
 - A formatted date is TEXT, so it does not sort or calculate correctly.
*/


/*
===============================================================================
 11. FORMAT   -> date/number to nicely formatted TEXT
===============================================================================

 Syntax: FORMAT(value, 'pattern')

 Pattern parts:
   yyyy = 4 digit year   yy = 2 digit year
   MM   = month number   MMM = Jan   MMMM = January     (M is CAPITAL)
   dd   = day number     ddd = Mon   dddd = Monday
   HH   = 24 hour        hh  = 12 hour   mm = minutes   ss = seconds
*/

SELECT order_date,
       FORMAT(order_date, 'dd-MM-yyyy')     AS indian_format,   -- 31-01-2024
       FORMAT(order_date, 'MM/dd/yyyy')     AS us_format,       -- 01/31/2024
       FORMAT(order_date, 'MMMM yyyy')      AS month_year,      -- January 2024
       FORMAT(order_date, 'MMM dd, yyyy')   AS short_form,      -- Jan 31, 2024
       FORMAT(order_date, 'dddd')           AS day_name         -- Wednesday
FROM   orders;

-- Numbers can be formatted too
SELECT FORMAT(1234567.891, 'N2') AS number_format,   -- 1,234,567.89
       FORMAT(0.256,       'P1') AS percent_format,  -- 25.6 %
       FORMAT(1234,        'C')  AS currency_format; -- $1,234.00

-- WARNING: FORMAT is easy but SLOW on large tables.
-- Use CONVERT with a style number when performance matters.


/*
===============================================================================
 12. CONVERT   -> change data type, with an optional STYLE for dates
===============================================================================

 Syntax: CONVERT(target_type, value, style_number)
 SQL Server only. Faster than FORMAT.

 Useful style numbers:
   23  -> yyyy-mm-dd        (ISO, best choice)
   105 -> dd-mm-yyyy
   101 -> mm/dd/yyyy
   103 -> dd/mm/yyyy
   112 -> yyyymmdd
   120 -> yyyy-mm-dd hh:mi:ss
*/

SELECT CONVERT(DATE, '2024-01-31')                  AS text_to_date,
       CONVERT(VARCHAR, GETDATE(), 23)              AS iso_text,
       CONVERT(VARCHAR, GETDATE(), 105)             AS indian_text,
       CONVERT(VARCHAR, GETDATE(), 120)             AS full_text,
       CONVERT(INT, '123')                          AS text_to_int;

-- Remove the time part from a datetime
SELECT CONVERT(DATE, GETDATE()) AS date_only;


/*
===============================================================================
 13. CAST   -> change data type (STANDARD SQL, works in every database)
===============================================================================

 Syntax: CAST(value AS target_type)
 No style option, but portable. Prefer CAST unless you need a date style.
*/

SELECT CAST('2024-01-31' AS DATE)         AS text_to_date,
       CAST(GETDATE()    AS DATE)         AS remove_time,
       CAST('123'        AS INT)          AS text_to_int,
       CAST(123          AS VARCHAR(10))  AS int_to_text,
       CAST(10 AS DECIMAL(10,2)) / 3      AS avoid_int_division;

-- TRY_CAST / TRY_CONVERT : returns NULL instead of an error on bad data
SELECT TRY_CAST('abc'        AS DATE) AS bad_value,   -- NULL, no error
       TRY_CAST('2024-01-31' AS DATE) AS good_value;

-- Very useful when cleaning raw text dates from a source file
SELECT order_date_text,
       TRY_CONVERT(DATE, order_date_text) AS clean_date
FROM   bronze.raw_orders;


/*
===============================================================================
 14. DATEADD   -> add or subtract time from a date
===============================================================================

 Syntax: DATEADD(part, number, date)
 A negative number subtracts.
*/

SELECT order_date,
       DATEADD(day,   10, order_date) AS plus_10_days,
       DATEADD(month,  1, order_date) AS next_month,
       DATEADD(year,  -1, order_date) AS last_year,
       DATEADD(hour,   5, order_date) AS plus_5_hours
FROM   orders;

-- Orders from the last 30 days
SELECT * FROM orders
WHERE  order_date >= DATEADD(day, -30, GETDATE());

-- First and last day of the CURRENT month
SELECT DATEADD(day, 1, EOMONTH(GETDATE(), -1)) AS month_start,
       EOMONTH(GETDATE())                      AS month_end;

-- Expected delivery date = order date + 7 days
SELECT order_id,
       order_date,
       DATEADD(day, 7, order_date) AS expected_delivery
FROM   orders;


/*
===============================================================================
 15. DATEDIFF   -> difference between two dates
===============================================================================

 Syntax: DATEDIFF(part, start_date, end_date)
 Returns end_date - start_date, as a whole number.
*/

SELECT order_date,
       ship_date,
       DATEDIFF(day,   order_date, ship_date) AS days_to_ship,
       DATEDIFF(month, order_date, ship_date) AS months_diff,
       DATEDIFF(year,  order_date, GETDATE()) AS years_ago
FROM   orders;

-- Average shipping delay per month
SELECT   DATETRUNC(month, order_date)                AS order_month,
         AVG(DATEDIFF(day, order_date, ship_date))   AS avg_delay_days
FROM     orders
GROUP BY DATETRUNC(month, order_date)
ORDER BY order_month;

-- Customer age
SELECT customer_name,
       DATEDIFF(year, birthdate, GETDATE()) AS age
FROM   customers;

-- WARNING: DATEDIFF counts BOUNDARIES crossed, not full periods.
-- DATEDIFF(year, '2024-12-31', '2025-01-01') = 1, even though it is only 1 day.


/*
===============================================================================
 16. ISDATE   -> check whether a text value is a valid date
===============================================================================

 Returns 1 = valid date, 0 = not a valid date.
 Works on text, used to validate dirty source data.
*/

SELECT ISDATE('2024-01-31') AS valid,      -- 1
       ISDATE('2024-13-45') AS invalid,    -- 0
       ISDATE('abc')        AS not_a_date; -- 0

-- Find bad date values in a raw bronze table
SELECT order_date_text
FROM   bronze.raw_orders
WHERE  ISDATE(order_date_text) = 0;

-- Load only the valid dates, put NULL for the bad ones
SELECT CASE WHEN ISDATE(order_date_text) = 1
            THEN CAST(order_date_text AS DATE)
            ELSE NULL
       END AS order_date
FROM   bronze.raw_orders;

-- MODERN AND BETTER: TRY_CAST does the same in one step
SELECT TRY_CAST(order_date_text AS DATE) AS order_date
FROM   bronze.raw_orders;


/*
===============================================================================
 QUICK SUMMARY
===============================================================================
 Function                  | Purpose                       | Returns
 --------------------------|-------------------------------|-------------
 GETDATE()                 | current date + time           | DATETIME
 YEAR/MONTH/DAY            | extract one part              | INT
 DATEPART(part, d)         | extract any part as number    | INT
 DATENAME(part, d)         | extract any part as text      | VARCHAR
 DATETRUNC(part, d)        | cut down to month/quarter/year| DATE
 EOMONTH(d)                | last day of the month         | DATE
 FORMAT(d, 'pattern')      | pretty text (slow)            | VARCHAR
 CONVERT(type, v, style)   | change type + date style      | any
 CAST(v AS type)           | change type (standard SQL)    | any
 TRY_CAST(v AS type)       | change type, NULL if invalid  | any
 DATEADD(part, n, d)       | add / subtract time           | DATE
 DATEDIFF(part, d1, d2)    | difference between two dates  | INT
 ISDATE(text)              | 1 = valid date, 0 = invalid   | INT

 Remember
 - Always write dates as 'YYYY-MM-DD'
 - Store as DATE, format only when displaying
 - Avoid functions on date columns in WHERE, use a date range instead
 - DATEDIFF counts boundaries, not complete periods
 - Prefer TRY_CAST over ISDATE for cleaning raw data
===============================================================================
*/
