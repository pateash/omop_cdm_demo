from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def device_exposure_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("DEVICE_EXPOSURE_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("DEVICE_CONCEPT_ID", LongType(), True),
            StructField("DEVICE_EXPOSURE_START_DATE", DateType(), True),
            StructField("DEVICE_EXPOSURE_START_DATETIME", TimestampType(), True),
            StructField("DEVICE_EXPOSURE_END_DATE", DateType(), True),
            StructField("DEVICE_EXPOSURE_END_DATETIME", TimestampType(), True),
            StructField("DEVICE_TYPE_CONCEPT_ID", LongType(), True),
            StructField("UNIQUE_DEVICE_ID", StringType(), True),
            StructField("QUANTITY", LongType(), True),
            StructField("PROVIDER_ID", LongType(), True),
            StructField("VISIT_OCCURRENCE_ID", LongType(), True),
            StructField("VISIT_DETAIL_ID", LongType(), True),
            StructField("DEVICE_SOURCE_VALUE", StringType(), True),
            StructField("DEVICE_SOURCE_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
