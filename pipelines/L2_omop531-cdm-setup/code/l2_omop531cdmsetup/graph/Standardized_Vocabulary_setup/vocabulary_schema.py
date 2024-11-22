from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def vocabulary_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("VOCABULARY_ID", StringType(), True),
            StructField("VOCABULARY_NAME", StringType(), True),
            StructField("VOCABULARY_REFERENCE", StringType(), True),
            StructField("VOCABULARY_VERSION", StringType(), True),
            StructField("VOCABULARY_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
