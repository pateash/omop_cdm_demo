from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def cohort_definition_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("cohort_definition_id", LongType(), True),
            StructField("cohort_definition_name", StringType(), True),
            StructField("cohort_definition_description", StringType(), True),
            StructField("definition_type_concept_id", LongType(), True),
            StructField("cohort_definition_syntax", StringType(), True),
            StructField("subject_concept_id", LongType(), True),
            StructField("cohort_initiation_date", DateType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
