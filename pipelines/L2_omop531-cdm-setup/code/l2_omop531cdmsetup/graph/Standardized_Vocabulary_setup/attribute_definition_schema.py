from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def attribute_definition_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("ATTRIBUTE_DEFINITION_ID", LongType(), True),
            StructField("ATTRIBUTE_NAME", StringType(), True),
            StructField("ATTRIBUTE_DESCRIPTION", StringType(), True),
            StructField("ATTRIBUTE_TYPE_CONCEPT_ID", LongType(), True),
            StructField("ATTRIBUTE_SYNTAX", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
