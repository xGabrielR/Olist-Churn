WITH filter_sellers AS (
	SELECT DISTINCT
		oi.order_id,
		oi.seller_id
	FROM orders o 
	LEFT JOIN order_items oi ON o.order_id = oi.order_id 
	WHERE o.order_purchase_timestamp < '2018-09-01'
	  AND o.order_purchase_timestamp >= '2017-06-01'
	  AND oi.seller_id IS NOT NULL
	  AND o.order_status = 'delivered'
	  AND JULIANDAY(o.order_delivered_customer_date) - JULIANDAY(o.order_approved_at) > 0
),
calc_monetary AS (
	SELECT 
		f.seller_id,
		DATE_FORMAT(order_purchase_timestamp, '%Y-%m') + '-01' AS order_month_purchase,
		price * order_item_id AS monetary
	FROM filter_sellers  f
	LEFT JOIN orders o ON f.order_id = o.order_id
	LEFT JOIN order_items i ON f.order_id = i.order_id AND i.seller_id = f.seller_id
),
monthly_seller_monetary AS (
	SELECT 
		seller_id,
		order_month_purchase,
		SUM(monetary) AS monetary
	FROM calc_monetary
	GROUP BY seller_id, order_month_purchase 
),
monthly_seller_ltv AS (
	SELECT 
		seller_id,
		order_month_purchase,
		SUM(monetary) OVER (PARTITION BY seller_id ORDER BY order_month_purchase) AS ltv
	FROM monthly_seller_monetary
),
all_available_months AS (
	SELECT DISTINCT order_month_purchase
	FROM calc_monetary
),
all_available_sellers_months AS (
	SELECT DISTINCT
		seller_id,
		order_month_purchase
	FROM all_available_months d 
	CROSS JOIN filter_sellers l
),
intermitent_monthly_ltv_time_series AS (
	SELECT 
		t.seller_id,
		t.order_month_purchase,
		COALESCE(s.ltv, 0) AS forecast_ltv
	FROM all_available_sellers_months t
	LEFT JOIN monthly_seller_ltv s ON t.order_month_purchase = s.order_month_purchase 
		  		                  AND t.seller_id = s.seller_id
)
SELECT * FROM intermitent_monthly_ltv_time_series
ORDER BY seller_id, order_month_purchase