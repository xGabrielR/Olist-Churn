with filter_orders as (
	select 
		*
	from orders
	where order_purchase_timestamp >= '2017-06-01'
	  and order_purchase_timestamp < '2018-09-01'
	  and order_status = 'created'
)
select 
	o.order_id,
	i.order_id as order_id_item,
	o.order_purchase_timestamp
from filter_orders o 
left join order_items i on i.order_id = o.order_id