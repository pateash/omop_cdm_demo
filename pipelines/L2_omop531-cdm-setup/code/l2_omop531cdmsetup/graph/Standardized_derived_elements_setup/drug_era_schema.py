from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def drug_era_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("DRUG_ERA_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("DRUG_CONCEPT_ID", LongType(), True),
            StructField("DRUG_ERA_START_DATE", DateType(), True),
            StructField("DRUG_ERA_END_DATE", DateType(), True),
            StructField("DRUG_EXPOSURE_COUNT", LongType(), True),
            StructField("GAP_DAYS", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
