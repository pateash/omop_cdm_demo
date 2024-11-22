from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def condition_era_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("CONDITION_ERA_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("CONDITION_CONCEPT_ID", LongType(), True),
            StructField("CONDITION_ERA_START_DATE", DateType(), True),
            StructField("CONDITION_ERA_END_DATE", DateType(), True),
            StructField("CONDITION_OCCURRENCE_COUNT", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
