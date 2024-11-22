from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def care_site_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("CARE_SITE_ID", LongType(), True),
            StructField("CARE_SITE_NAME", StringType(), True),
            StructField("PLACE_OF_SERVICE_CONCEPT_ID", LongType(), True),
            StructField("LOCATION_ID", LongType(), True),
            StructField("CARE_SITE_SOURCE_VALUE", StringType(), True),
            StructField("PLACE_OF_SERVICE_SOURCE_VALUE", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
