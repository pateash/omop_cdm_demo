from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_cdm_setup.config.ConfigStore import *
from l1_cdm_setup.udfs.UDFs import *

def extract_table_name_from_path_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("path").alias("source_path"), 
        regexp_extract(col("path"), "/([^/]+).csv.gz$", 1).alias("target_table"), 
        col("modificationTime").alias("timestamp")
    )
