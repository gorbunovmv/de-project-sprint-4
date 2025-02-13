DROP TABLE IF EXISTS cte1_temp;

CREATE TEMP TABLE cte1_temp as
WITH cte1 AS ( 
	SELECT 
		c.week_of_year,
		FS.item_id AS item_new,
		FS.customer_Id,
		sum(payment_amount) AS payment_amount
	FROM mart.f_sales fs
		JOIN mart.d_calendar c ON FS.date_id = c.date_id
	WHERE 
		quantity > 0
		AND customer_id IN (SELECT customer_id 
							FROM mart.f_sales 
							WHERE quantity > 0 
							GROUP BY customer_id HAVING count(*)=1)
	GROUP BY 	
		c.week_of_year,
		FS.item_id,
		FS.customer_Id
)
SELECT 
	week_of_year,
	item_new,
	count(DISTINCT customer_id) AS new_customers_count, 
	sum(payment_amount) AS new_customers_revenue
FROM cte1
GROUP BY week_of_year,item_new;

DROP TABLE IF EXISTS cte2_temp;

CREATE TEMP TABLE cte2_temp as
WITH cte2 AS ( 
	SELECT 
		c.week_of_year,
		FS.item_id AS item_returning, 
		FS.customer_id,
		sum(FS.payment_amount) AS payment_amount
	FROM mart.f_sales fs
		JOIN mart.d_calendar c ON FS.date_id = c.date_id
		FULL JOIN (SELECT customer_id
				   FROM mart.f_sales
				   WHERE quantity > 0
				   GROUP BY customer_id
				   HAVING count(*)>1) f ON f.customer_id = FS.customer_id
	WHERE 
		quantity > 0
   GROUP BY 	
		c.week_of_year,
		FS.item_id,
		FS.customer_id
)
SELECT	week_of_year, 
	    item_returning,
		count(DISTINCT customer_id) AS returning_customers_count, 
		sum(payment_amount) AS returning_customers_revenue  
FROM cte2
GROUP BY week_of_year, item_returning;

--------------------------------------------------------------------------
DROP TABLE IF EXISTS cte3_temp;

CREATE TEMP TABLE cte3_temp as
WITH cte3 AS 
( 
	SELECT 
		c.week_of_year,
		FS.item_id AS item_refunding, 
		FS.customer_id,
		sum(FS.payment_amount) AS payment_amount
	FROM mart.f_sales fs
		JOIN mart.d_calendar c ON FS.date_id = c.date_id
		FULL JOIN (SELECT customer_id
				   FROM mart.f_sales
				   WHERE quantity < 0
				   GROUP BY customer_id) f ON f.customer_id = FS.customer_id
	WHERE 
		quantity < 0
   GROUP BY 	
		c.week_of_year,
		FS.item_id,
		FS.customer_id
)
SELECT	week_of_year, 
	    item_refunding,
		count(DISTINCT customer_id) AS refunded_customer_count, 
		count(customer_id) AS customers_refunded  
FROM cte3
GROUP BY week_of_year, item_refunding;

INSERT INTO mart.f_customer_retention (
	new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	period_id,
	item_id,      
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded
)
SELECT	c1.new_customers_count,
		c2.returning_customers_count,
		c3.refunded_customer_count,
		c1.week_of_year,
		coalesce(c1.item_new, c2.item_returning) AS item_id,
		c1.new_customers_revenue,
		c2.returning_customers_revenue,
		c3.customers_refunded
	FROM cte1_temp c1
		FULL JOIN cte2_temp c2 ON c1.week_of_year = c2.week_of_year AND c1.item_new = c2.item_returning
		FULL JOIN cte3_temp c3 ON c1.week_of_year = c3.week_of_year AND c1.item_new = c3.item_refunding;
