-- North wind traders is a shipping company looking explore their sales performance anually, our goal is to create a database, ETL process & provide valuable insights for the business

-- Create a schema northwindtraders  
CREATE DATABASE northwindtraders;
USE northwindtraders;
-- Extract data from 7 tables into the database from their respective csv files
-- transform the tables e.g. and detecting data types for the columns

-- The below code is the process of converting a 'date' string column to a 'date' format in mySql
		ALTER TABLE orders CHANGE COLUMN `orderDate` `order_date` VARCHAR(20);
		UPDATE orders SET order_date = date_format(str_to_date(order_date, '%Y/%m/%d'), '%Y-%m-%d');
		SELECT CAST(order_date AS date) FROM orders;
		ALTER TABLE orders MODIFY COLUMN `order_date` date;

		ALTER TABLE orders CHANGE COLUMN `requiredDate` `required_date` VARCHAR(20);
		UPDATE orders SET required_date = date_format(str_to_date(required_date, '%Y/%m/%d'), '%Y-%m-%d');
		SELECT CAST(required_date AS date) FROM orders;
		ALTER TABLE orders MODIFY COLUMN `required_date` date;

		ALTER TABLE orders CHANGE COLUMN `shippedDate` `shipped_date` VARCHAR(20);
		UPDATE orders SET shipped_date = date_format(str_to_date(shipped_date, '%Y/%m/%d'), '%Y-%m-%d');
		SELECT CAST(shipped_date AS date) FROM orders;
		ALTER TABLE orders MODIFY COLUMN `shipped_date` date;

-- Identify primary keys and foreign keys for table relationships 

SELECT * FROM categories;

-- Q1 Calculate Total Orders By year
SELECT
YEAR(order_date) AS yr,
COUNT(orders.order_id) AS total_orders
-- customer_id AS customers
FROM 
orders
INNER JOIN 
order_details
ON 
order_details.order_id = orders.order_id
GROUP BY 1
ORDER BY 1;

-- Q2 Calculate highest Orders By customer ID & country
SELECT DISTINCT
COUNT(orders.order_id) AS total_orders,
(customer_id) AS customers
FROM 
orders
LEFT JOIN 
order_details
ON 
order_details.order_id = orders.order_id
GROUP BY 2
ORDER BY 1 DESC;

-- Q3 Calculate total products in each categories
SELECT
(category_name) As cat,
COUNT(products.category_id) AS total_products
FROM 
categories
RIGHT OUTER JOIN
products
ON 
products.category_id = categories.category_id 
GROUP BY 1
ORDER BY 2 DESC;

-- Q4 What was the overall freight costs incurred by each company
SELECT 
(shippers.company_name) AS shipping_co,
ROUND(SUM(freight),0) AS total_freight
FROM
orders
INNER JOIN shippers
ON 
orders.shipper_id = shippers.shipper_id
GROUP BY 1
ORDER BY 2 DESC;


-- Q5 What was the total sales by each product after discount?
SELECT
(products.product_name) AS prod_name,
ROUND(SUM(order_details.unit_price),2) - ROUND(SUM(order_details.discount),2) AS sales
FROM
order_details
INNER JOIN products
ON
order_details.product_id = products.product_id
GROUP BY 1
ORDER BY 2 DESC;

-- Q6 Use the same context and break in down by Year and categories

CREATE TEMPORARY TABLE total_sales_cat
SELECT
	YEAR(o.order_date) AS yr,
	c.category_name AS cat_name,
    -- p.product_id AS p,
    p.product_name AS prod_name,
    ROUND(SUM(od.unit_price - od.discount), 2) AS sales
FROM
    order_details od
INNER JOIN
    products p ON od.product_id = p.product_id
LEFT JOIN
    orders o ON od.order_id = o.order_id
INNER JOIN
	categories c ON p.category_id = c.category_id
GROUP BY
    p.product_id,
    YEAR(o.order_date)
ORDER BY
    p.product_id,
    yr;
-- In this step we will use unpivot method to show year as separate columns
-- Strangely MySql does not support PIVOT/UNPIVOT functions, thanks CHATgpt for saving my time!
    -- SELECT
--     total_sales_cat.yr,
--     total_sales_cat.cat_name,
--     total_sales_cat.prod_name,
--     total_sales_cat.sales
--     FROM 
--     total_sales_cat
--     UNPIVOT 
-- 	( 
-- 	SUM(total_sales_cat.sales) FOR total_sales_cat.yr IN (total_sales_cat.cat_name, total_sales_cat.prod_name ) 
-- 	)AS PivotTable;
    
    -- Chatgpt version 
    SELECT
    cat_name,
    prod_name,
    SUM(CASE WHEN yr = 2013 THEN sales ELSE 0 END) AS `2013`,
    SUM(CASE WHEN yr = 2014 THEN sales ELSE 0 END) AS `2014`,
    SUM(CASE WHEN yr = 2015 THEN sales ELSE 0 END) AS `2015`
FROM
    total_sales_cat
GROUP BY
    cat_name,
    prod_name
ORDER BY
    cat_name,
    prod_name;
    

-- Q7  Do some Gap Analysis of orders to find out the TAT period and to which country the order got most delayed?
SELECT 
  -- (orders.order_id) i,
  (customers.country) c,
  (orders.customer_id) cust,
  order_date,
  required_date,
  shipped_date,
  DATEDIFF(shipped_date, order_date) AS days_to_ship, -- days difference between order date and shipped date 
  DATEDIFF(required_date, shipped_date) AS days_to_delivery, -- days difference between required date and shipped date
  DATEDIFF(shipped_date, order_date) / DATEDIFF(required_date, shipped_date) * 100 AS closure_rt 
  FROM 
  orders
LEFT JOIN 
	order_details ON orders.order_id = order_details.order_id
INNER JOIN 
	customers ON customers.customer_id = orders.customer_id
GROUP BY c
ORDER BY closure_rt;

-- Q8 Let us do some deep dive on products which are discontinued but has some potential among the customers by orders
SELECT
-- product_id p,
(categories.category_name) AS cat_name,
product_name,
-- category_id,
COUNT(order_details.order_id)  AS total_orders,
ROUND(SUM(order_details.unit_price) - SUM(order_details.discount)) AS total_sales,
-- discontinued AS NA
CASE WHEN products.discontinued = 1 THEN 'Yes' ELSE NULL END AS discontinued_products
FROM 
products
LEFT JOIN order_details ON order_details.product_id = products.product_id
RIGHT OUTER JOIN categories ON categories.category_id = products.category_id
INNER JOIN orders ON orders.order_id = order_details.order_id
WHERE discontinued = 1
GROUP BY product_name
ORDER BY total_sales DESC;

-- Q9 what about employees? Can we use Q7 and pull out records of employees who processed those orders? 
SELECT  
  -- (orders.order_id) i,
  (customers.country) customer_country,
  (orders.customer_id) cust,
  (employees.employee_name) employee_nme,
  order_date,
  required_date,
  shipped_date,
  DATEDIFF(shipped_date, order_date) AS days_to_ship, -- days difference between order date and shipped date 
  DATEDIFF(required_date, shipped_date) AS days_to_delivery, -- days difference between required date and shipped date
  DATEDIFF(shipped_date, order_date) / DATEDIFF(required_date, shipped_date) * 100 AS closure_rt 
  FROM 
  orders
LEFT JOIN 
	order_details ON orders.order_id = order_details.order_id
INNER JOIN 
	customers ON customers.customer_id = orders.customer_id
LEFT JOIN 
	employees ON employees.employee_id = orders.employee_id
GROUP BY customer_country
ORDER BY closure_rt;

-- Q 10 Let us find which categories got us the most revenue and from which country? 
SELECT DISTINCT
-- (o.customer_id) AS x,
(c.country) Customer_country,
(p.product_name) Name_of_product,
(ct.category_name) Category_Product,
COUNT(o.order_id) AS Total_orders,
ROUND(SUM(od.unit_price) - SUM(od.discount), 2) As Total_sales_After_Discount
FROM 
orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN order_details od ON od.order_id = o.order_id
INNER JOIN products p ON od.product_id = p.product_id
INNER JOIN categories ct ON ct.category_id = p.category_id

GROUP BY Customer_country
ORDER BY Total_sales_After_Discount DESC;





