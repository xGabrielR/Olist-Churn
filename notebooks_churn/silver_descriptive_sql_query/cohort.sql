WITH cohort AS (
	SELECT DISTINCT
		oi.order_id,
		oi.seller_id,
		STRFTIME('%Y-%m', DATE(o.order_purchase_timestamp)) || '-01' AS date
	FROM orders o 
	LEFT JOIN order_items oi ON o.order_id = oi.order_id 
	WHERE o.order_purchase_timestamp >= '2017-06-01'
	  AND o.order_purchase_timestamp < '2018-10-01'
	  AND oi.seller_id IS NOT NULL
	  AND o.order_status = 'delivered'
	  AND DATEDIFF(o.order_delivered_customer_date, o.order_approved_at) > 0
),
cohort_date AS (
	SELECT 
		seller_id,
		MIN(date) AS cohort_date
	FROM cohort
	GROUP BY seller_id
),
cohort_index AS (
	SELECT 
		c.seller_id,
		d.cohort_date,
		c.date,
		EXTRACT(YEAR FROM c.date) - EXTRACT(YEAR FROM d.cohort_date) AS year_diff,
		EXTRACT(MONTH FROM c.date) - EXTRACT(MONTH FROM d.cohort_date) AS month_diff
	FROM cohort c
	LEFT JOIN cohort_date d ON c.seller_id = d.seller_id
)
SELECT
	cohort_date,
	year_diff * 12 + month_diff + 1 AS cohort_index,
	COUNT(DISTINCT seller_id) AS seller_id
FROM cohort_index
GROUP BY cohort_date, cohort_index