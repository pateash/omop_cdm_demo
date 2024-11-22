from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def observation_period_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("OBSERVATION_PERIOD_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("OBSERVATION_PERIOD_START_DATE", DateType(), True),
            StructField("OBSERVATION_PERIOD_END_DATE", DateType(), True),
            StructField("PERIOD_TYPE_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
