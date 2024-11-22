from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def dose_era_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("DOSE_ERA_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("DRUG_CONCEPT_ID", LongType(), True),
            StructField("UNIT_CONCEPT_ID", LongType(), True),
            StructField("DOSE_VALUE", DoubleType(), True),
            StructField("DOSE_ERA_START_DATE", DateType(), True),
            StructField("DOSE_ERA_END_DATE", DateType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
