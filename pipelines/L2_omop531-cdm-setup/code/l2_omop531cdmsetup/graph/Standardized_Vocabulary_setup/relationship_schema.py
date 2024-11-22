from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def relationship_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("RELATIONSHIP_ID", StringType(), True),
            StructField("RELATIONSHIP_NAME", StringType(), True),
            StructField("IS_HIERARCHICAL", StringType(), True),
            StructField("DEFINES_ANCESTRY", StringType(), True),
            StructField("REVERSE_RELATIONSHIP_ID", StringType(), True),
            StructField("RELATIONSHIP_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
