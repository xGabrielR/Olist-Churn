with agg_seller as (
	select 
		order_id,
		seller_id,
		MAX(price * order_item_id) as total_price
	from order_items i
	group by 1, 2
),
ltv as (
	select 
		i.seller_id,
		sum(i.total_price) as ltv
	from orders o
	inner join agg_seller i on i.order_id = o.order_id 
	where order_purchase_timestamp >= '2017-06-01'
	  and order_purchase_timestamp < '2018-09-01'
	  and order_status = 'delivered'
	group by 1
)
select 
	seller_id,
	ltv,
	cast((count(*) over(order by ltv desc)) as float) / (select count(*) from ltv) as pareto_rank
from ltv
group by 1
limit 25