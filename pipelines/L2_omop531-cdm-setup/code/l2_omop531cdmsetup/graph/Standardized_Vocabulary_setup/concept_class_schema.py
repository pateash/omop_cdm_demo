from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def concept_class_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("CONCEPT_CLASS_ID", StringType(), True),
            StructField("CONCEPT_CLASS_NAME", StringType(), True),
            StructField("CONCEPT_CLASS_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
