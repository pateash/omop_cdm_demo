from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def payer_plan_period_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("PAYER_PLAN_PERIOD_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("PAYER_PLAN_PERIOD_START_DATE", DateType(), True),
            StructField("PAYER_PLAN_PERIOD_END_DATE", DateType(), True),
            StructField("PAYER_CONCEPT_ID", LongType(), True),
            StructField("PAYER_SOURCE_VALUE", StringType(), True),
            StructField("PAYER_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("PLAN_CONCEPT_ID", LongType(), True),
            StructField("PLAN_SOURCE_VALUE", StringType(), True),
            StructField("PLAN_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("SPONSOR_CONCEPT_ID", LongType(), True),
            StructField("SPONSOR_SOURCE_VALUE", StringType(), True),
            StructField("SPONSOR_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("FAMILY_SOURCE_VALUE", StringType(), True),
            StructField("STOP_REASON_CONCEPT_ID", LongType(), True),
            StructField("STOP_REASON_SOURCE_VALUE", StringType(), True),
            StructField("STOP_REASON_SOURCE_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
