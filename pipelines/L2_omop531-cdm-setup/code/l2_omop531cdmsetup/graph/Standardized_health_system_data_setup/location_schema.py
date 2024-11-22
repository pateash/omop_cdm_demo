from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def location_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("LOCATION_ID", LongType(), True),
            StructField("ADDRESS_1", StringType(), True),
            StructField("ADDRESS_2", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("ZIP", StringType(), True),
            StructField("COUNTY", StringType(), True),
            StructField("LOCATION_SOURCE_VALUE", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
