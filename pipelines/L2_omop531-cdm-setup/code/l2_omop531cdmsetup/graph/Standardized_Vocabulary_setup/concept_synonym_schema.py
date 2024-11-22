from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def concept_synonym_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("CONCEPT_ID", LongType(), True),
            StructField("CONCEPT_SYNONYM_NAME", StringType(), True),
            StructField("LANGUAGE_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
