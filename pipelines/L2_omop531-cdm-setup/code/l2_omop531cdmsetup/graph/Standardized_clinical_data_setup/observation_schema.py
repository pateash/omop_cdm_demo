from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def observation_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("OBSERVATION_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("OBSERVATION_CONCEPT_ID", LongType(), True),
            StructField("OBSERVATION_DATE", DateType(), True),
            StructField("OBSERVATION_DATETIME", TimestampType(), True),
            StructField("OBSERVATION_TYPE_CONCEPT_ID", LongType(), True),
            StructField("VALUE_AS_NUMBER", DoubleType(), True),
            StructField("VALUE_AS_STRING", StringType(), True),
            StructField("VALUE_AS_CONCEPT_ID", LongType(), True),
            StructField("QUALIFIER_CONCEPT_ID", LongType(), True),
            StructField("UNIT_CONCEPT_ID", LongType(), True),
            StructField("PROVIDER_ID", LongType(), True),
            StructField("VISIT_OCCURRENCE_ID", LongType(), True),
            StructField("VISIT_DETAIL_ID", LongType(), True),
            StructField("OBSERVATION_SOURCE_VALUE", StringType(), True),
            StructField("OBSERVATION_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("UNIT_SOURCE_VALUE", StringType(), True),
            StructField("QUALIFIER_SOURCE_VALUE", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
