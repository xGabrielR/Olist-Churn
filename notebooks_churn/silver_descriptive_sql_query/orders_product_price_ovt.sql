select 
	o.order_id,
	i.product_id,
	o.order_purchase_timestamp,
	i.freight_value,
	i.seller_id,
	i.price
from order_items i 
inner join orders o on o.order_id  = i.order_id 
where product_id = 'dd113cb02b2af9c8e5787e8f1f0722f6'