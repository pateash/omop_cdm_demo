from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def fact_relationship_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("DOMAIN_CONCEPT_ID_1", LongType(), True),
            StructField("FACT_ID_1", LongType(), True),
            StructField("DOMAIN_CONCEPT_ID_2", LongType(), True),
            StructField("FACT_ID_2", LongType(), True),
            StructField("RELATIONSHIP_CONCEPT_ID", LongType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
