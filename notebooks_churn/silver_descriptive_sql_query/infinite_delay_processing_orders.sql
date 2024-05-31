select 
	order_id,
	customer_id,
	order_purchase_timestamp,
	order_approved_at,
	order_estimated_delivery_date,
	datediff('2018-09-01', order_approved_at) as days_in_processing
from orders
where order_purchase_timestamp >= '2017-06-01'
  and order_purchase_timestamp < '2018-09-01'
  and order_status = 'processing'
order by order_purchase_timestamp