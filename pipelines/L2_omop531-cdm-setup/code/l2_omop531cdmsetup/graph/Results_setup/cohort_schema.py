from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def cohort_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("cohort_definition_id", LongType(), True),
            StructField("subject_id", LongType(), True),
            StructField("cohort_start_date", DateType(), True),
            StructField("cohort_end_date", DateType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
