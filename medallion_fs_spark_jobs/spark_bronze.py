import sys
import logging
from pytz import timezone
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as pf
from pyspark.sql.dataframe import DataFrame

BRONZE_WORKFLOW = {
    "metadata": {
        "_mt_bronze_extraction_tool": "dms",
        "_mt_bronze_source_system": "render_oltp",
        "_mt_bronze_workflow_created_by": "Gabriel R."
    },
    "tables": [
        "postgres_public_orders",
        "postgres_public_order_items"
    ]
}

def check_bronze_table_exists(
    table_name: str,
    spark: SparkSession
) -> bool:
    try:
        spark.sql(f"SELECT * FROM catalog.grc_iceberg_bronze.{table_name} LIMIT 1")
        return True

    except:
        return False
    
def check_bronze_partition_date(
    table_name: str,
    spark: SparkSession
) -> bool:
    last_dt = spark.sql(f"""
        SELECT MAX(_mt_bronze_partition_date) AS df FROM
        catalog.grc_iceberg_bronze.{table_name}
    """).collect()[0][0]

    if last_dt == datetime.now(timezone("America/Sao_Paulo")).strftime("%Y-%m-%d"):
        return True
    
    else:
        return False
    
def read_bronze_dataframe(
    landing_table_path: str,
    spark: SparkSession
) -> DataFrame:
    try:
        df = spark.read.format("parquet").load(landing_table_path)
        return df
        
    except Exception as e:
        print(e)
        return None
    
def fix_bronze_void_columns(
    df: DataFrame
) -> DataFrame:
    void_columns = [k[0] for k in df.dtypes if k[-1] == 'void']
    for void_col in void_columns:
        df = df.withColumn(void_col, pf.col(void_col).cast("string").alias(void_col))

    return df

def check_dataframe(
    df: DataFrame,
    table_name: str
) -> None:
    if str(df.schema).__contains__("_corrupt_record"):
        raise Exception(f"A Tabela: '{table_name}' contem dados corrompidos da landing.")

    if df.head(1):
        return None

    else:
        raise Exception(f"A Tabela: '{table_name}' não tem nenhuma linha da landing.")

def generate_bronze_metadata(
    df: DataFrame,
    metadata: dict
) -> DataFrame:
    created_by = metadata["_mt_bronze_workflow_created_by"]
    source_system = metadata["_mt_bronze_source_system"]
    extraction_tool = metadata["_mt_bronze_extraction_tool"]

    df = df.withColumn(
        "_mt_bronze_ingestion_time",
        pf.lit(datetime.now(timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S"))
    )
    
    df = df.withColumn(
        "_mt_bronze_partition_date",
        pf.lit(datetime.now(timezone('America/Sao_Paulo')).strftime("%Y-%m-%d"))
    )

    df = df.withColumn(
        "_mt_bronze_dataframe_schema",
        pf.lit(str(dict(df.dtypes)))
    )

    df = df.withColumn(
        "_mt_bronze_workflow_created_by",
        pf.lit(created_by)
    )
    
    df = df.withColumn(
        "_mt_bronze_extraction_tool",
        pf.lit(extraction_tool)
    )
    
    df = df.withColumn(
        "_mt_bronze_source_system",
        pf.lit(source_system)
    )
    
    return df

def process_bronze_dataframe(
    landing_table_path: str,
    table_name: str,
    metadata: dict,
    spark: SparkSession
) -> DataFrame:    
    df = read_bronze_dataframe(landing_table_path, spark=spark)

    if df:
        check_dataframe(df, table_name)
    
        df = fix_bronze_void_columns(df)
        df = generate_bronze_metadata(df, metadata=metadata)
        
        df = df.coalesce(1)
        
        return df
    
    else:
        return None

if __name__ == "__main__":
    logging.basicConfig(
        format="""%(asctime)s,%(msecs)d %(levelname)-8s[%(filename)s:%(funcName)s:%(lineno)d] %(message)s""",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    spark = SparkSession.builder.getOrCreate()

    for bronze_table_name in BRONZE_WORKFLOW["tables"]:
        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | STARTING BRONZE: {bronze_table_name}")

        source_system, schema = bronze_table_name.split("_")[0], bronze_table_name.split("_")[1]
        original_table_name = "_".join(bronze_table_name.split("_")[2:])

        df = process_bronze_dataframe(
            spark=spark,
            table_name=bronze_table_name,
            metadata=BRONZE_WORKFLOW["metadata"],
            landing_table_path=f"s3a://grc-lh-landing/{source_system}/{schema}/{original_table_name}",
        )

        if df:
            if check_bronze_table_exists(table_name=bronze_table_name, spark=spark):
                if check_bronze_partition_date(table_name=bronze_table_name, spark=spark):
                    dt = datetime.now(timezone('America/Sao_Paulo')).strftime("%Y-%m-%d")
                    spark.sql(f"DELETE FROM catalog.grc_iceberg_bronze.{bronze_table_name} WHERE _mt_bronze_partition_date = '{dt}'")

                    df.writeTo(f"catalog.grc_iceberg_bronze.{bronze_table_name}")\
                        .using("iceberg")\
                        .partitionedBy("_mt_bronze_partition_date")\
                        .tableProperty("write.format.default", "parquet")\
                        .tableProperty("write.distribution-mode", "hash")\
                        .tableProperty("write.parquet.dict-size-bytes", "134217728")\
                        .append()
                    
                else:
                    df.writeTo(f"catalog.grc_iceberg_bronze.{bronze_table_name}")\
                        .using("iceberg")\
                        .partitionedBy("_mt_bronze_partition_date")\
                        .tableProperty("write.format.default", "parquet")\
                        .tableProperty("write.distribution-mode", "hash")\
                        .tableProperty("write.parquet.dict-size-bytes", "134217728")\
                        .append()
            
            else:
                df.writeTo(f"catalog.grc_iceberg_bronze.{bronze_table_name}")\
                    .using("iceberg")\
                    .partitionedBy("_mt_bronze_partition_date")\
                    .tableProperty("write.format.default", "parquet")\
                    .tableProperty("write.distribution-mode", "hash")\
                    .tableProperty("write.parquet.dict-size-bytes", "134217728")\
                    .create()
                
        d = datetime.now(tz=timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{d} | ENDED BRONZE: {bronze_table_name}")
