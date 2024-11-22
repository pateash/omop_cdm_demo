from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def cohort_attribute_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("COHORT_DEFINITION_ID", LongType(), True),
            StructField("SUBJECT_ID", LongType(), True),
            StructField("COHORT_START_DATE", DateType(), True),
            StructField("COHORT_END_DATE", DateType(), True),
            StructField("ATTRIBUTE_DEFINITION_ID", LongType(), True),
            StructField("VALUE_AS_NUMBER", DoubleType(), True),
            StructField("VALUE_AS_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
