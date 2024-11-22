from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def metadata_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("METADATA_CONCEPT_ID", LongType(), True),
            StructField("METADATA_TYPE_CONCEPT_ID", LongType(), True),
            StructField("NAME", StringType(), True),
            StructField("VALUE_AS_STRING", StringType(), True),
            StructField("VALUE_AS_CONCEPT_ID", LongType(), True),
            StructField("METADATA_DATE", DateType(), True),
            StructField("METADATA_DATETIME", TimestampType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
