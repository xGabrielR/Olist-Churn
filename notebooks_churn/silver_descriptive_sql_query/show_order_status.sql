select 
	order_status,
	count(distinct order_id)
from orders
where order_purchase_timestamp >= '2017-06-01'
  and order_purchase_timestamp < '2018-09-01'
group by 1
order by 2