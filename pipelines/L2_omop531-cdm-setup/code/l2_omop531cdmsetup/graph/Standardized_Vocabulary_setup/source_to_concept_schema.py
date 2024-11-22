from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def source_to_concept_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("SOURCE_CODE", StringType(), True),
            StructField("SOURCE_CONCEPT_ID", LongType(), True),
            StructField("SOURCE_VOCABULARY_ID", StringType(), True),
            StructField("SOURCE_CODE_DESCRIPTION", StringType(), True),
            StructField("TARGET_CONCEPT_ID", LongType(), True),
            StructField("TARGET_VOCABULARY_ID", StringType(), True),
            StructField("VALID_START_DATE", DateType(), True),
            StructField("VALID_END_DATE", DateType(), True),
            StructField("INVALID_REASON", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
