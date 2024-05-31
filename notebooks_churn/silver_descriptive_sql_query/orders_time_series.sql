with ym as (
	select 
		substr(date(order_purchase_timestamp), 0, 8) as year_month,
		order_id 
	from orders o 
)
select 
	year_month,
	count(order_id)
from ym
group by 1
order by 1 desc