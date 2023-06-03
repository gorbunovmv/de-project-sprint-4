CREATE TABLE mart.f_customer_retention (
	new_customers_count int NULL,
	returning_customers_count int NULL,
	refunded_customer_count int NULL,
	period_name varchar(10) DEFAULT 'weekly',
	period_id int NOT NULL,
	item_id int	NOT null,      
	new_customers_revenue int NULL,
	returning_customers_revenue int NULL,
	customers_refunded int NULL
)
