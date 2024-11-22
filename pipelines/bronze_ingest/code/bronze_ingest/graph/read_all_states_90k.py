from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bronze_ingest.config.ConfigStore import *
from bronze_ingest.udfs.UDFs import *

def read_all_states_90k(spark: SparkSession) -> DataFrame:
    out0 = spark.createDataFrame(dbutils.fs.ls('s3://hls-eng-data-public/data/rwe/all-states-90K'))

    return out0
