"""
This module contains spark utility functions like get_spark_session etc.
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

conf = os.getenv("CONF", "base")

running_environment = os.getenv("RUNNING_ENVIRONMENT", "dev")
PROJECT_NAME = "spaceflights_17_4"


def get_spark_session() -> SparkSession:
    """
    Purpose: To create a spark session with below properties
    :return:
    """
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
    # pyarrow is not working so disable it for now
    spark.conf.set("spark.sql.execution.arrow.enabled", "false")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_spark_empty_df(schema=None) -> DataFrame:
    """
    Purpose: This is a helper function which is used to create and return an empty dataset.
    It can be used at multiple places in the code wherever required.
    :return:
    """
    if schema is None:
        schema = StructType([])
    spark = get_spark_session()
    src = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return src
