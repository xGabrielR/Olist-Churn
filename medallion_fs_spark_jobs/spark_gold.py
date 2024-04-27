import sys
import logging
from pytz import timezone
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as pf
from pyspark.sql.dataframe import DataFrame

GOLD_WORKFLOW = {
    "postgres_public_sellers_rfm": {
        "key": "overwrite",
        "query": """
            WITH metrics AS (
                SELECT
                    i.seller_id,
                    DATEDIFF(MAX(o.order_purchase_timestamp), MIN(o.order_purchase_timestamp)) AS recency,
                    COUNT(DISTINCT o.order_id) AS frequency,
                    MAX(i.price * i.order_item_id) AS monetary
                FROM catalog.grc_iceberg_silver.postgres_public_order_items i
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_orders o ON o.order_id = i.order_id
                WHERE i.seller_id IS NOT NULL
                GROUP BY seller_id
            )
            SELECT 
                seller_id,
                recency,
                frequency,
                monetary,
                COALESCE(frequency / recency, 0) AS frequency_recency,
                monetary / frequency AS monetary_frequency
            FROM metrics
        """
    },
    "postgres_public_sellers_price_selling_days": {
        "key": "overwrite",
        "query": """
            WITH seller_time_series AS (
                SELECT 
                    i.seller_id,
                    DATE(o.order_purchase_timestamp) as date_selled,
                    MAX(price * order_item_id) AS monetary
                FROM catalog.grc_iceberg_silver.postgres_public_order_items i
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_orders o ON o.order_id = i.order_id
                WHERE i.seller_id IS NOT NULL
                GROUP BY 1, 2
            ),
            seller_lag AS (
                SELECT 
                    seller_id,
                    date_selled,
                    monetary,
                    LAG(date_selled) OVER (PARTITION BY seller_id ORDER BY date_selled) AS lag_date,
                    LAG(monetary) OVER (PARTITION BY seller_id ORDER BY date_selled) AS lag_price
                FROM seller_time_series
            )
            SELECT
                seller_id,
                date_selled,
                monetary,
                lag_date,
                lag_price,
                DATEDIFF(date_selled, lag_date) AS diff_selling_days,
                monetary - lag_price AS diff_monetary
            FROM seller_lag
        """
    },
    "postgres_public_sellers_intermitent_time_series": {
        "key": "overwrite",
        "query": """
            WITH all_dates AS (
                SELECT DISTINCT DATE(order_purchase_timestamp) AS date
                FROM catalog.grc_iceberg_silver.postgres_public_orders ORDER BY date ASC
            ),
            all_sellers AS (
                SELECT DISTINCT seller_id
                FROM catalog.grc_iceberg_silver.postgres_public_order_items
            ),
            time_series_sellers_dates AS (
                SELECT 
                    seller_id,
                    date
                FROM all_sellers
                CROSS JOIN all_dates
            ),
            sellers AS (
                SELECT 
                    seller_id,
                    DATE(order_purchase_timestamp) AS order_purchase_timestamp,
                    SUM(price * order_item_id) AS monetary
                FROM catalog.grc_iceberg_silver.postgres_public_orders o 
                LEFT JOIN catalog.grc_iceberg_silver.postgres_public_order_items i ON i.order_id = o.order_id
                WHERE i.seller_id IS NOT NULL
                GROUP BY 1, 2
            ),
            seller_time_series AS (
                SELECT 
                    t.seller_id,
                    t.date,
                    COALESCE(s.monetary, 0) AS monetary
                FROM time_series_sellers_dates t
                LEFT JOIN sellers s ON t.date = s.order_purchase_timestamp AND t.seller_id = s.seller_id
            ),
            sellers_lags AS (
                SELECT 
                    seller_id,
                    date,
                    monetary,
                    LAG(monetary) OVER (PARTITION BY seller_id ORDER BY date) AS lag_monetary
                FROM seller_time_series
            )
            SELECT
                seller_id,
                date,
                monetary,
                monetary - lag_monetary AS diff_monetary
            FROM sellers_lags
        """
    }
}

query_merge_table = """
MERGE INTO {0}.{1}.{2} a
USING new_silver_rows b
ON {3}

WHEN MATCHED THEN 
    UPDATE SET *

WHEN NOT MATCHED THEN 
    INSERT *
"""

def generate_merge_key(
    update_key: str
) -> str:
    if update_key not in ["overwrite", "append"]:

        if update_key.__contains__(","):
            return " and ".join([f"a.{k} = b.{k}" for k in update_key.split(",")])

        else:
            return f"a.{update_key} = b.{update_key}"

    return update_key

def generate_gold_metadata(
    df: DataFrame,
    update_key: str
) -> DataFrame:
    df = df.withColumn(
        "_mt_gold_ingestion_date",
        pf.current_date()
    )

    df = df.withColumn(
        "_mt_gold_update_key",
        pf.lit(update_key)
    )
    
    df = df.withColumn(
        "_mt_gold_written_lines",
        pf.lit(df.count())
    )
    
    return df

def process_gold_dataframe(
    spark: object,
    gold_properties: dict[str]
) -> DataFrame:
    df = spark.sql(gold_properties["query"])
    df = generate_gold_metadata(df, update_key=gold_properties["key"])

    return df

if __name__ == "__main__":
    logging.basicConfig(
        format="""%(asctime)s,%(msecs)d %(levelname)-8s[%(filename)s:%(funcName)s:%(lineno)d] %(message)s""",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    spark = SparkSession.builder.getOrCreate()

    for gold_table_name, properties in GOLD_WORKFLOW.items():
        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | STARTED GOLD: {gold_table_name}")

        df = process_gold_dataframe(
            spark=spark,
            gold_properties=properties
        )

        update_key = generate_merge_key(update_key=properties["key"])

        if update_key == "overwrite":
            df.writeTo(f"catalog.grc_iceberg_gold.{gold_table_name}")\
                .using("iceberg")\
                .tableProperty("location", f"s3a://grc-lh-gold/grc_iceberg_gold.db/{gold_table_name}")\
                .tableProperty("write.format.default", "parquet")\
                .tableProperty("write.parquet.dict-size-bytes", "134217728")\
                .tableProperty("write.distribution-mode", "hash")\
                .createOrReplace()

        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | STARTED GOLD: {gold_table_name}")