with filter_orders as (
	select 
		order_id,
		customer_id
	from orders
	where order_purchase_timestamp >= '2017-06-01'
	  and order_purchase_timestamp < '2018-09-01'
	  and order_status = 'created'
)
select * from orders o
inner join filter_orders f on o.customer_id = f.customer_id