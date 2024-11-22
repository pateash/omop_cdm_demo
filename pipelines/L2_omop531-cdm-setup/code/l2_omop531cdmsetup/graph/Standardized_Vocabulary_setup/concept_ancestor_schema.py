from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def concept_ancestor_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("ANCESTOR_CONCEPT_ID", LongType(), True),
            StructField("DESCENDANT_CONCEPT_ID", LongType(), True),
            StructField("MIN_LEVELS_OF_SEPARATION", LongType(), True),
            StructField("MAX_LEVELS_OF_SEPARATION", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
