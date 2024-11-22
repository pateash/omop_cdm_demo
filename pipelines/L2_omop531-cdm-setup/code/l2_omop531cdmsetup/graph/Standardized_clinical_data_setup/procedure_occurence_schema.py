from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def procedure_occurence_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("PROCEDURE_OCCURRENCE_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("PROCEDURE_CONCEPT_ID", LongType(), True),
            StructField("PROCEDURE_DATE", DateType(), True),
            StructField("PROCEDURE_DATETIME", TimestampType(), True),
            StructField("PROCEDURE_TYPE_CONCEPT_ID", LongType(), True),
            StructField("MODIFIER_CONCEPT_ID", LongType(), True),
            StructField("QUANTITY", LongType(), True),
            StructField("PROVIDER_ID", LongType(), True),
            StructField("VISIT_OCCURRENCE_ID", LongType(), True),
            StructField("VISIT_DETAIL_ID", LongType(), True),
            StructField("PROCEDURE_SOURCE_VALUE", StringType(), True),
            StructField("PROCEDURE_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("MODIFIER_SOURCE_VALUE", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
