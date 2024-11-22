from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def provider_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("PROVIDER_ID", LongType(), True),
            StructField("PROVIDER_NAME", StringType(), True),
            StructField("NPI", StringType(), True),
            StructField("DEA", StringType(), True),
            StructField("SPECIALTY_CONCEPT_ID", LongType(), True),
            StructField("CARE_SITE_ID", LongType(), True),
            StructField("YEAR_OF_BIRTH", LongType(), True),
            StructField("GENDER_CONCEPT_ID", LongType(), True),
            StructField("PROVIDER_SOURCE_VALUE", StringType(), True),
            StructField("SPECIALTY_SOURCE_VALUE", StringType(), True),
            StructField("SPECIALTY_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("GENDER_SOURCE_VALUE", StringType(), True),
            StructField("GENDER_SOURCE_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
