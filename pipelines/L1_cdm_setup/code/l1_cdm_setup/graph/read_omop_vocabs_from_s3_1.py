from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_cdm_setup.config.ConfigStore import *
from l1_cdm_setup.udfs.UDFs import *

def read_omop_vocabs_from_s3_1(spark: SparkSession) -> DataFrame:
    out0 = spark.createDataFrame(dbutils.fs.ls('s3://hls-eng-data-public/data/rwe/omop-vocabs'))

    return out0
