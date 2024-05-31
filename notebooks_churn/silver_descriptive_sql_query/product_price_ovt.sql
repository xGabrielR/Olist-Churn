with product_price as (
	select distinct
		product_id,
		price
	from order_items
),
price_variation as (
	select 
		product_id,
		min(price) as min_price,
		avg(price) as avg_price,
		max(price) as max_price
	from product_price
	group by 1
)
select * from price_variation
where min_price != max_price
order by avg_price desc