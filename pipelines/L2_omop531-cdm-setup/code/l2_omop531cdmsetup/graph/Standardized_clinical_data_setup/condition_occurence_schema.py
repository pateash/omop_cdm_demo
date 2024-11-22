from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def condition_occurence_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("CONDITION_OCCURRENCE_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("CONDITION_CONCEPT_ID", LongType(), True),
            StructField("CONDITION_START_DATE", DateType(), True),
            StructField("CONDITION_START_DATETIME", TimestampType(), True),
            StructField("CONDITION_END_DATE", DateType(), True),
            StructField("CONDITION_END_DATETIME", TimestampType(), True),
            StructField("CONDITION_TYPE_CONCEPT_ID", LongType(), True),
            StructField("STOP_REASON", StringType(), True),
            StructField("PROVIDER_ID", LongType(), True),
            StructField("VISIT_OCCURRENCE_ID", LongType(), True),
            StructField("VISIT_DETAIL_ID", LongType(), True),
            StructField("CONDITION_SOURCE_VALUE", StringType(), True),
            StructField("CONDITION_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("CONDITION_STATUS_SOURCE_VALUE", StringType(), True),
            StructField("CONDITION_STATUS_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
