from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def measurement_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("MEASUREMENT_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("MEASUREMENT_CONCEPT_ID", LongType(), True),
            StructField("MEASUREMENT_DATE", DateType(), True),
            StructField("MEASUREMENT_DATETIME", TimestampType(), True),
            StructField("MEASUREMENT_TIME", StringType(), True),
            StructField("MEASUREMENT_TYPE_CONCEPT_ID", LongType(), True),
            StructField("OPERATOR_CONCEPT_ID", LongType(), True),
            StructField("VALUE_AS_NUMBER", DoubleType(), True),
            StructField("VALUE_AS_CONCEPT_ID", LongType(), True),
            StructField("UNIT_CONCEPT_ID", LongType(), True),
            StructField("RANGE_LOW", DoubleType(), True),
            StructField("RANGE_HIGH", DoubleType(), True),
            StructField("PROVIDER_ID", LongType(), True),
            StructField("VISIT_OCCURRENCE_ID", LongType(), True),
            StructField("VISIT_DETAIL_ID", LongType(), True),
            StructField("MEASUREMENT_SOURCE_VALUE", StringType(), True),
            StructField("MEASUREMENT_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("UNIT_SOURCE_VALUE", StringType(), True),
            StructField("VALUE_SOURCE_VALUE", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
