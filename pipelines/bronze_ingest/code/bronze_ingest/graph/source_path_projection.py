from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bronze_ingest.config.ConfigStore import *
from bronze_ingest.udfs.UDFs import *

def source_path_projection(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("path").alias("source_path"), 
        regexp_extract(col("path"), "([^/]+)/?$", 1).alias("target_table"), 
        col("modificationTime").cast(TimestampType()).alias("timestamp")
    )
