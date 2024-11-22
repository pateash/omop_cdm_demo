from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def concept_relationship_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("CONCEPT_ID_1", LongType(), True),
            StructField("CONCEPT_ID_2", LongType(), True),
            StructField("RELATIONSHIP_ID", StringType(), True),
            StructField("VALID_START_DATE", DateType(), True),
            StructField("VALID_END_DATE", DateType(), True),
            StructField("INVALID_REASON", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
