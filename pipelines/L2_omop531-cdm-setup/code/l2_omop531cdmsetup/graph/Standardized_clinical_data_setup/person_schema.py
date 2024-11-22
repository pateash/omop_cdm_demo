from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def person_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("PERSON_ID", LongType(), True),
            StructField("GENDER_CONCEPT_ID", LongType(), True),
            StructField("YEAR_OF_BIRTH", LongType(), True),
            StructField("MONTH_OF_BIRTH", LongType(), True),
            StructField("DAY_OF_BIRTH", LongType(), True),
            StructField("BIRTH_DATETIME", TimestampType(), True),
            StructField("RACE_CONCEPT_ID", LongType(), True),
            StructField("ETHNICITY_CONCEPT_ID", LongType(), True),
            StructField("LOCATION_ID", LongType(), True),
            StructField("PROVIDER_ID", LongType(), True),
            StructField("CARE_SITE_ID", LongType(), True),
            StructField("PERSON_SOURCE_VALUE", StringType(), True),
            StructField("GENDER_SOURCE_VALUE", StringType(), True),
            StructField("GENDER_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("RACE_SOURCE_VALUE", StringType(), True),
            StructField("RACE_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("ETHNICITY_SOURCE_VALUE", StringType(), True),
            StructField("ETHNICITY_SOURCE_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
