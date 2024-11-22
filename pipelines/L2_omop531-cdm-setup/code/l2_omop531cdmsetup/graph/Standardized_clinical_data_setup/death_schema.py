from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def death_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("PERSON_ID", LongType(), True),
            StructField("DEATH_DATE", DateType(), True),
            StructField("DEATH_DATETIME", TimestampType(), True),
            StructField("DEATH_TYPE_CONCEPT_ID", LongType(), True),
            StructField("CAUSE_CONCEPT_ID", LongType(), True),
            StructField("CAUSE_SOURCE_VALUE", StringType(), True),
            StructField("CAUSE_SOURCE_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
