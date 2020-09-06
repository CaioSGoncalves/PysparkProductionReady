from pyspark.sql.functions import col, expr
from pyspark.sql.types import *

def _extract_data(spark, config):
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("value", FloatType(), True),
        StructField("timestamp", DateType(), True)
    ])

    return (
        spark.read.format("csv")
        .option("header", "True")
        .schema(schema)
        .load("/hdfs/raw/transaction")
    )


def _transform_data(spark, raw_df):
    raw_df.createOrReplaceTempView("transaction")

    spark.sql("""
        SELECT
            user_id,
            timestamp,
            
            CASE
                WHEN (type = 'SAQUE') THEN (-1 * value)
                ELSE value
            END as value
            
        FROM transaction
    """).createOrReplaceTempView("transaction")

    return spark.sql("""
        SELECT
            user_id,
            SUM(value) as balance
            
        FROM transaction

        GROUP BY 1
    """)


def _load_data(config, transformed_df):
    (
        transformed_df.write.format("parquet")
        .mode("overwrite")
        .save("/hdfs/processed/transaction/")
    )


def run_job(spark, config):
    _load_data(config, _transform_data(spark, _extract_data(spark, config)))