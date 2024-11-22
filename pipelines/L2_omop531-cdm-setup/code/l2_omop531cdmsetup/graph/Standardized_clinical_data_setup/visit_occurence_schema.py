from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def visit_occurence_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("VISIT_OCCURRENCE_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("VISIT_CONCEPT_ID", LongType(), True),
            StructField("VISIT_START_DATE", DateType(), True),
            StructField("VISIT_START_DATETIME", TimestampType(), True),
            StructField("VISIT_END_DATE", DateType(), True),
            StructField("VISIT_END_DATETIME", TimestampType(), True),
            StructField("VISIT_TYPE_CONCEPT_ID", LongType(), True),
            StructField("PROVIDER_ID", LongType(), True),
            StructField("CARE_SITE_ID", LongType(), True),
            StructField("VISIT_SOURCE_VALUE", StringType(), True),
            StructField("VISIT_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("ADMITTING_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("ADMITTING_SOURCE_VALUE", StringType(), True),
            StructField("DISCHARGE_TO_CONCEPT_ID", LongType(), True),
            StructField("DISCHARGE_TO_SOURCE_VALUE", StringType(), True),
            StructField("PRECEDING_VISIT_OCCURRENCE_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
