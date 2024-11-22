from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def concept_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("CONCEPT_ID", LongType(), True),
            StructField("CONCEPT_NAME", StringType(), True),
            StructField("DOMAIN_ID", StringType(), True),
            StructField("VOCABULARY_ID", StringType(), True),
            StructField("CONCEPT_CLASS_ID", StringType(), True),
            StructField("STANDARD_CONCEPT", StringType(), True),
            StructField("CONCEPT_CODE", StringType(), True),
            StructField("VALID_START_DATE", DateType(), True),
            StructField("VALID_END_DATE", DateType(), True),
            StructField("INVALID_REASON", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
