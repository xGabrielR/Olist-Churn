import sys
import logging
from pytz import timezone
from datetime import datetime

from pyspark.sql import SparkSession

INGESTION_FEATURE_STORES = {
    "fs_seller_purchases": {
        "query": """
            WITH fs_filter_sellers AS (
                SELECT DISTINCT
                    oi.order_id,
                    oi.seller_id
                FROM catalog.grc_iceberg_silver.postgres_public_orders o 
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_order_items oi ON o.order_id = oi.order_id 
                WHERE o.order_purchase_timestamp < '{date}'
                AND o.order_purchase_timestamp >= ADD_MONTHS('{date}', -6)
                AND oi.seller_id  IS NOT NULL
            ),
            fs_business AS (
                SELECT 
                    f.seller_id,
                    i.order_id,
                    i.order_item_id,
                    i.product_id,
                    i.shipping_limit_date,
                    o.order_purchase_timestamp,
                    i.price
                FROM fs_filter_sellers f
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_order_items i ON i.order_id = f.order_id
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_orders o ON o.order_id = f.order_id
            ),
            fs_descriptions AS (
                SELECT 
                    seller_id,
                    MIN(DATEDIFF('{date}', DATE(order_purchase_timestamp))) AS recency,
                    SUM(price) / COUNT(product_id) as avg_ticket,        -- need a fix to use monetary price * quantity 
                    COUNT(product_id) / COUNT(DISTINCT order_id) AS avg_order_product,
                    MAX(price * order_item_id) AS max_order_items_price, --gross_revenue, order feature
                    MIN(price * order_item_id) AS min_order_items_price,
                    AVG(price * order_item_id) AS avg_order_items_price,
                    AVG(price) AS avg_item_price,
                    MAX(price) AS max_item_price,
                    MIN(price) AS min_item_price,
                    COUNT(DISTINCT DATE(order_purchase_timestamp)) AS quantity_date_orders,
                    COUNT(product_id) AS quantity_products_selled,
                    COUNT(DISTINCT product_id) AS quantity_unique_product_selled
                FROM fs_business
                -- WHERE seller_id  LIKE 'd1c2%' check this user
                GROUP BY seller_id
            ),
            fs_seller_life AS (
                SELECT
                    oi.seller_id,
                    SUM(oi.price) AS ltv,
                    MAX(DATEDIFF('{date}', DATE(order_purchase_timestamp))) AS days_from_first_sell,
                    COUNT(DISTINCT o.order_id) / (DATEDIFF(MAX(order_purchase_timestamp), MIN(order_purchase_timestamp))) AS frequency
                FROM catalog.grc_iceberg_silver.postgres_public_orders o 
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_order_items oi ON o.order_id = oi.order_id 
                WHERE o.order_purchase_timestamp < '{date}'
                AND oi.seller_id IS NOT NULL
                GROUP BY oi.seller_id
            ),
            fs_seller_date AS (
                SELECT DISTINCT
                    seller_id,
                    DATE(order_purchase_timestamp) AS order_purchase_timestamp
                FROM fs_business 
                ORDER BY seller_id, order_purchase_timestamp
            ),
            fs_lag_seller AS (
                SELECT 
                    seller_id,
                    order_purchase_timestamp,
                    LAG(order_purchase_timestamp) OVER (PARTITION BY seller_id ORDER BY order_purchase_timestamp) AS lag1
                FROM fs_seller_date
            ),
            fs_seller_interval AS (
                SELECT 
                    seller_id,
                    AVG(DATEDIFF(order_purchase_timestamp, lag1)) AS avg_seller_interval
                FROM fs_lag_seller
                GROUP BY seller_id
            )
            SELECT 
                '{date}' AS _mt_date_reference,
                NOW() AS _mt_ingestion_date,
                a.seller_id,
                a.recency,
                a.avg_ticket,
                a.avg_order_product,
                a.max_order_items_price,
                a.min_order_items_price,
                a.avg_order_items_price,
                a.max_item_price,
                a.min_item_price,
                a.avg_item_price,
                a.quantity_date_orders,
                a.quantity_products_selled,
                a.quantity_unique_product_selled,
                b.avg_seller_interval,
                c.ltv,
                c.days_from_first_sell,
                c.frequency
            FROM fs_descriptions a
            LEFT JOIN fs_seller_interval b ON b.seller_id = a.seller_id
            INNER JOIN fs_seller_life c ON c.seller_id = a.seller_id
        """
    },
    "fs_seller_orders": {
        "query": """
            /*
            * Maybe i need to change order_purchase_timestamp to 
            * order_delivered_customer_date because i can have
            * order_delivered_customer_date greather than 2018-01-01 !!!
            * Looks like data leak.
            */
            WITH fs_filter_sellers AS (
                SELECT DISTINCT
                    oi.order_id,
                    oi.seller_id
                FROM catalog.grc_iceberg_silver.postgres_public_orders o 
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_order_items oi ON o.order_id = oi.order_id 
                WHERE o.order_purchase_timestamp < '{date}'
                AND o.order_purchase_timestamp >= ADD_MONTHS('{date}', -6)
                AND oi.seller_id  IS NOT NULL
            ),
            /*
                The order_id = 00143d0f86d6fbd9f9b38ab440ac16f5 has 3 items
                (same product). Each item has the freight calculated accordingly
                to its measures and weight.
                To get the total freight value for each order you just have to sum.
                The total order_item value is: 21.33 * 3 = 63.99
                The total freight value is: 15.10 * 3 = 45.30
            */
            fs_business AS (
                SELECT 
                    f.seller_id,
                    o.order_id,
                    o.order_status,
                    o.order_purchase_timestamp,
                    o.order_approved_at,
                    o.order_delivered_customer_date,
                    o.order_estimated_delivery_date,
                    -- shipping_limit_date,
                    SUM(i.price) AS price,
                    SUM(i.freight_value) AS freight_value 
                FROM fs_filter_sellers f
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_orders o ON o.order_id = f.order_id
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_order_items i ON i.order_id = f.order_id
                GROUP BY 
                    f.seller_id,
                    o.order_id,
                    o.order_status,
                    o.order_purchase_timestamp,
                    o.order_approved_at,
                    o.order_delivered_customer_date,
                    o.order_estimated_delivery_date
            ), 
            fs_cleaning AS (
                SELECT 
                    seller_id,
                    order_id,
                    order_status,
                    order_purchase_timestamp,
                    order_approved_at,
                    order_estimated_delivery_date,
                    freight_value,
                    price,
                    COALESCE(order_delivered_customer_date, '{date}') AS order_delivered_customer_date,
                    CASE WHEN order_status = 'DELIVERED' THEN true ELSE false END AS flag_delivered,
                    CASE WHEN order_status = 'CANCELED' THEN true ELSE false END AS flag_canceled
                FROM fs_business
            ),
            fs_descriptions AS (
                SELECT 
                    seller_id,
                    AVG(freight_value) AS avg_freight_value,
                    MIN(freight_value) AS min_freight_value,
                    MAX(freight_value) AS max_freight_value,
                    AVG(DATEDIFF(order_delivered_customer_date, order_approved_at)) AS avg_approval_delivery_days,
                    AVG(DATEDIFF(order_approved_at, order_purchase_timestamp)) AS avg_approval_purchase_days,
                    MIN(DATEDIFF(order_delivered_customer_date, order_approved_at)) AS min_approval_delivery_days,
                    MAX(DATEDIFF(order_approved_at, order_purchase_timestamp)) AS max_approval_purchase_days,
                    MAX(DATEDIFF(order_delivered_customer_date, order_approved_at)) AS max_approval_delivery_days,
                    MIN(DATEDIFF(order_approved_at, order_purchase_timestamp)) AS min_approval_purchase_days,
                    CAST(COUNT(flag_delivered) AS FLOAT) / CAST(COUNT(DISTINCT order_id) AS FLOAT) AS pct_status_delivered,
                    CAST(COUNT(flag_canceled) AS FLOAT) / CAST(COUNT(DISTINCT order_id) AS FLOAT) AS pct_status_canceled,
                    CAST(COUNT(CASE WHEN flag_delivered AND DATE(order_delivered_customer_date) > DATE(order_estimated_delivery_date) THEN order_id END) AS FLOAT) / CAST(COUNT(CASE WHEN flag_delivered THEN order_id END) AS FLOAT) AS pct_delay_delivered_orders
                FROM fs_cleaning
                GROUP BY seller_id
            )
            SELECT 
                '{date}' AS _mt_date_reference,
                NOW() AS _mt_ingestion_date,
                seller_id,
                avg_freight_value,
                min_freight_value,
                max_freight_value,
                avg_approval_delivery_days,
                min_approval_delivery_days,
                max_approval_delivery_days,
                avg_approval_purchase_days,
                min_approval_purchase_days,
                max_approval_purchase_days,
                pct_status_delivered,
                pct_status_canceled,
                pct_delay_delivered_orders
            FROM fs_descriptions
        """
    }
}

INGESTION_ABT = {
    "abt_seller_churn": {
        "query": """
            WITH fs_features AS (
                SELECT 
                    a._mt_date_reference,
                    a.seller_id,
                    a.recency,
                    a.avg_ticket,
                    a.avg_order_product,
                    a.max_order_items_price,
                    a.min_order_items_price,
                    a.avg_order_items_price,
                    a.max_item_price,
                    a.min_item_price,
                    a.avg_item_price,
                    a.quantity_date_orders,
                    a.quantity_products_selled,
                    a.quantity_unique_product_selled,
                    a.avg_seller_interval,
                    a.ltv,
                    a.days_from_first_sell,
                    a.frequency,
                    b.avg_freight_value,
                    b.min_freight_value,
                    b.max_freight_value,
                    b.avg_approval_delivery_days,
                    b.min_approval_delivery_days,
                    b.max_approval_delivery_days,
                    b.avg_approval_purchase_days,
                    b.min_approval_purchase_days,
                    b.max_approval_purchase_days,
                    b.pct_status_delivered,
                    b.pct_status_canceled,
                    b.pct_delay_delivered_orders
                FROM catalog.grc_iceberg_analytics.fs_seller_purchases a
                LEFT JOIN catalog.grc_iceberg_analytics.fs_seller_orders b ON a.seller_id = b.seller_id AND a._mt_date_reference = b._mt_date_reference
                WHERE a.recency <= 45
            ),
            abt_seller_activate AS (
                SELECT DISTINCT
                    oi.seller_id,
                    DATE(o.order_purchase_timestamp) AS date_sell
                FROM catalog.grc_iceberg_silver.postgres_public_orders o 
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_order_items oi ON o.order_id = oi.order_id 
                WHERE oi.seller_id IS NOT NULL
            ),
            abt_seller_flag AS (
                SELECT 
                    a.seller_id,
                    a._mt_date_reference,
                    MIN(b.date_sell) AS date_sell
                FROM fs_features a
                LEFT JOIN abt_seller_activate b ON a.seller_id = b.seller_id
                AND a._mt_date_reference <= b.date_sell
                AND datediff(b.date_sell, a._mt_date_reference) <= 45 - a.recency
                GROUP BY 1, 2
            )
            SELECT
                b.*,
                CASE WHEN a.date_sell IS NULL THEN 1 ELSE 0 END AS churn
            FROM abt_seller_flag a
            LEFT JOIN fs_features b ON a.seller_id = b.seller_id AND a._mt_date_reference = b._mt_date_reference
        """
    }
}

def check_fs_table_exists(
    table_name: str,
    spark: SparkSession
) -> bool:
    try:
        spark.sql(f"SELECT * FROM catalog.grc_iceberg_analytics.{table_name} LIMIT 1")
        return True

    except:
        return False

def get_fs_reference_date(
    table_name: str,
    spark: SparkSession
) -> list[str]:
    ingested_dates = spark.sql(f"""
        SELECT DISTINCT _mt_date_reference AS df FROM
        catalog.grc_iceberg_analytics.{table_name}
    """).collect()

    return [date[0] for date in ingested_dates]
    

if __name__ == "__main__":
    logging.basicConfig(
        format="""%(asctime)s,%(msecs)d %(levelname)-8s[%(filename)s:%(funcName)s:%(lineno)d] %(message)s""",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    spark = SparkSession.builder.getOrCreate()

    # Olist is between 2018-01-01 -> 2018-10-01 available data
    # I need a better logic for incremental months
    available_months = [datetime(2018, i, 1).strftime("%Y-%m-%d") for i in range(1, 10)]

    for fs_table_name, properties in INGESTION_FEATURE_STORES.items():
        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | STARTING FEATURE STORE: {fs_table_name}")

        if not check_fs_table_exists(table_name=fs_table_name, spark=spark):
            d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"{d} | CREATING FEATURE STORE: {fs_table_name}")

            df = spark.sql(properties["query"].format(date=available_months[0]))
            df.writeTo(f"catalog.grc_iceberg_analytics.{fs_table_name}")\
                    .using("iceberg")\
                    .partitionedBy("_mt_date_reference")\
                    .tableProperty("location", f"s3a://grc-lh-analytics/grc_iceberg_analytics.db/{fs_table_name}")\
                    .tableProperty("write.format.default", "parquet")\
                    .tableProperty("write.distribution-mode", "hash")\
                    .tableProperty("write.parquet.dict-size-bytes", "134217728")\
                    .create()

            for available_month in available_months[1:]:
                d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
                logging.info(f"{d} | INGESTION FEATURE STORE: {fs_table_name} -> {available_month}")

                df = spark.sql(properties["query"].format(date=available_month))
                df.writeTo(f"catalog.grc_iceberg_analytics.{fs_table_name}")\
                    .using("iceberg")\
                    .tableProperty("location", f"s3a://grc-lh-analytics/grc_iceberg_analytics.db/{fs_table_name}")\
                    .tableProperty("write.format.default", "parquet")\
                    .tableProperty("write.distribution-mode", "hash")\
                    .tableProperty("write.parquet.dict-size-bytes", "134217728")\
                    .append()

        else:
            # Add extra logic if UPSERT
            existing_month_references = get_fs_reference_date(table_name=fs_table_name, spark=spark)
            for existing_month in existing_month_references:
                if existing_month not in available_months:
                    d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
                    logging.info(f"{d} | INGESTION FEATURE STORE: {fs_table_name} -> {available_month}")

                    df = spark.sql(properties["query"].format(date=available_month))
                    df.writeTo(f"catalog.grc_iceberg_analytics.{fs_table_name}")\
                        .using("iceberg")\
                        .tableProperty("location", f"s3a://grc-lh-analytics/grc_iceberg_analytics.db/{fs_table_name}")\
                        .tableProperty("write.format.default", "parquet")\
                        .tableProperty("write.distribution-mode", "hash")\
                        .tableProperty("write.parquet.dict-size-bytes", "134217728")\
                        .append()

        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | ENDED FEATURE STORE: {fs_table_name}")
        
    for abt_table_name, properties in INGESTION_ABT.items():
        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | STARTING ABT: {abt_table_name}")

        df = spark.sql(properties["query"])
        df.writeTo(f"catalog.grc_iceberg_analytics.{abt_table_name}")\
            .using("iceberg")\
            .tableProperty("location", f"s3a://grc-lh-analytics/grc_iceberg_analytics.db/{abt_table_name}")\
            .tableProperty("write.format.default", "parquet")\
            .tableProperty("write.distribution-mode", "hash")\
            .tableProperty("write.parquet.dict-size-bytes", "134217728")\
            .createOrReplace()

        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | ENDED ABT: {abt_table_name}")