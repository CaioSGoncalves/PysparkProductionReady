from pyspark.sql import SparkSession

# df.printSchema()
# df.show()

def _extract_data(spark, config):
    return (
        spark.read.format("parquet")
        .load("/hdfs/test/input/")
    )


def _transform_data(raw_df):
    return raw_df


def _load_data(config, transformed_df):
    """ Save data to parquet file """
    (
        transformed_df.write
        .format("parquet")
        .mode("overwrite")
        .save("/hdfs/test/output/")
    )


def run_job(spark, config):
    """ Run movies job """
    _load_data(config, _transform_data(_extract_data(spark, config)))