import sys
import logging
from pytz import timezone
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as pf
from pyspark.sql.dataframe import DataFrame

SILVER_WORKFLOW = {
    "postgres_public_orders": {
        "bronze_table": "postgres_public_orders",
        "key": "overwrite",
        "query": """
            SELECT 
                order_id,
                customer_id,
                UPPER(TRIM(order_status)) AS order_status,
                TO_TIMESTAMP(order_approved_at) AS order_approved_at,
                TO_TIMESTAMP(order_delivered_carrier_date) AS order_delivered_carrier_date,
                TO_TIMESTAMP(order_delivered_customer_date) AS order_delivered_customer_date,
                TO_TIMESTAMP(order_estimated_delivery_date) AS order_estimated_delivery_date,
                TO_TIMESTAMP(order_purchase_timestamp) AS order_purchase_timestamp,
                
                _mt_bronze_ingestion_time,
                _mt_bronze_partition_date,
                _mt_bronze_dataframe_schema,
                _mt_bronze_written_lines,
                _mt_bronze_extraction_tool,
                _mt_bronze_source_system 
            FROM tbl
        """
    },
    "postgres_public_order_items": {
        "bronze_table": "postgres_public_order_items",
        "key": "overwrite",
        "query": """
            SELECT
                order_id,
                order_item_id,
                product_id,
                seller_id,
                CAST(price AS DOUBLE) AS price,
                CAST(freight_value AS DOUBLE) AS freight_value,
                TO_TIMESTAMP(shipping_limit_date) AS shipping_limit_date,
                
                _mt_bronze_ingestion_time,
                _mt_bronze_partition_date,
                _mt_bronze_dataframe_schema,
                _mt_bronze_written_lines,
                _mt_bronze_extraction_tool,
                _mt_bronze_source_system
            FROM tbl  
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

def get_silver_incremental(
    table_name: str,
    spark: SparkSession
) -> DataFrame:
    bronze_incremental_query = f"""
    SELECT DISTINCT * FROM catalog.grc_iceberg_bronze.{table_name}
    WHERE _mt_bronze_partition_date == (
        SELECT MAX(_mt_bronze_partition_date) AS dt
        FROM catalog.grc_iceberg_bronze.{table_name}
    )
    """

    return spark.sql(bronze_incremental_query)

def check_silver_table_exists(
    table_name: str,
    spark: SparkSession
) -> bool:
    try:
        spark.sql(f"SELECT * FROM catalog.grc_iceberg_silver.{table_name} LIMIT 1")
        return True

    except:
        return False
    
def generate_silver_metadata(
    df: DataFrame,
    update_key: str
) -> DataFrame:
    df = df.withColumn(
        "_mt_silver_ingestion_date",
        pf.current_date()
    )

    df = df.withColumn(
        "_mt_silver_update_key",
        pf.lit(update_key)
    )
    
    return df
    
def process_silver_dataframe(
    spark: object,
    silver_properties: dict[str]
) -> DataFrame:
    df = get_silver_incremental(
        table_name=silver_properties["bronze_table"],
        spark=spark
    )

    df.createOrReplaceTempView("tbl")
    df = spark.sql(silver_properties["query"])
    df = generate_silver_metadata(df, update_key=silver_properties["key"])

    return df

if __name__ == "__main__":
    logging.basicConfig(
        format="""%(asctime)s,%(msecs)d %(levelname)-8s[%(filename)s:%(funcName)s:%(lineno)d] %(message)s""",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    spark = SparkSession.builder.getOrCreate()

    for silver_table_name, properties in SILVER_WORKFLOW.items():
        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | STARTED SILVER: {silver_table_name}")

        df = process_silver_dataframe(
            spark=spark,
            silver_properties=properties
        )

        update_key = generate_merge_key(update_key=properties["key"])

        if not check_silver_table_exists(table_name=silver_table_name, spark=spark):
            df.writeTo(f"catalog.grc_iceberg_silver.{silver_table_name}")\
                .using("iceberg")\
                .tableProperty("location", f"s3a://grc-lh-silver/grc_iceberg_silver.db/{silver_table_name}")\
                .tableProperty("write.format.default", "parquet")\
                .tableProperty("write.parquet.dict-size-bytes", "134217728")\
                .tableProperty("write.distribution-mode", "hash")\
                .createOrReplace()
            
        else:
            if update_key == "overwrite":
                df.writeTo(f"catalog.grc_iceberg_silver.{silver_table_name}")\
                    .using("iceberg")\
                    .tableProperty("location", f"s3a://grc-lh-silver/grc_iceberg_silver.db/{silver_table_name}")\
                    .tableProperty("write.format.default", "parquet")\
                    .tableProperty("write.parquet.dict-size-bytes", "134217728")\
                    .tableProperty("write.distribution-mode", "hash")\
                    .createOrReplace()
                
            else:            
                df.createOrReplaceTempView("new_silver_rows")

                spark.sql(
                    query_merge_table.format(
                        "catalog",
                        "grc_iceberg_silver",
                        silver_table_name,
                        update_key
                    )
                )

        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | ENDED SILVER: {silver_table_name}")
