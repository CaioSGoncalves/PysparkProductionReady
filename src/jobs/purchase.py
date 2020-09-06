from pyspark.sql.functions import col, expr
from pyspark.sql.types import *

def _extract_data(spark, config):
    schema = StructType([
        StructField("purchase_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("timestamp", DateType(), True)
    ])

    return (
        spark.read.format("csv")
        .option("header", "True")
        .schema(schema)
        .load("/hdfs/raw/purchase")
    )


def _transform_data(spark, raw_df):
    raw_df.createOrReplaceTempView("purchase")

    return spark.sql("""
        SELECT 
            purchase_id,
            user_id,
            product_id,
            quantity,
            timestamp,
            
            DAY(timestamp) as day,
            MONTH(timestamp) as month,
            YEAR(timestamp) as year
            
        FROM purchase
    """)


def _load_data(config, transformed_df):
    (
        transformed_df.write.format("parquet")
        .mode("append")
        .save("/hdfs/processed/purchase/")
    )


def run_job(spark, config):
    _load_data(config, _transform_data(spark, _extract_data(spark, config)))