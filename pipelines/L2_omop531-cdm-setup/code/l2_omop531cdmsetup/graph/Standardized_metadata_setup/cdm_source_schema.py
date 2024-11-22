from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def cdm_source_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("CDM_SOURCE_NAME", StringType(), True),
            StructField("CDM_SOURCE_ABBREVIATION", StringType(), True),
            StructField("CDM_HOLDER", StringType(), True),
            StructField("SOURCE_DESCRIPTION", StringType(), True),
            StructField("SOURCE_DOCUMENTATION_REFERENCE", StringType(), True),
            StructField("CDM_ETL_REFERENCE", StringType(), True),
            StructField("SOURCE_RELEASE_DATE", DateType(), True),
            StructField("CDM_RELEASE_DATE", DateType(), True),
            StructField("CDM_VERSION", StringType(), True),
            StructField("VOCABULARY_VERSION", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
