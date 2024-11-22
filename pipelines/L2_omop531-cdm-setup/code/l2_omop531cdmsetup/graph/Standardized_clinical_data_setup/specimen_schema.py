from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def specimen_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("SPECIMEN_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("SPECIMEN_CONCEPT_ID", LongType(), True),
            StructField("SPECIMEN_TYPE_CONCEPT_ID", LongType(), True),
            StructField("SPECIMEN_DATE", DateType(), True),
            StructField("SPECIMEN_DATETIME", TimestampType(), True),
            StructField("QUANTITY", DoubleType(), True),
            StructField("UNIT_CONCEPT_ID", LongType(), True),
            StructField("ANATOMIC_SITE_CONCEPT_ID", LongType(), True),
            StructField("DISEASE_STATUS_CONCEPT_ID", LongType(), True),
            StructField("SPECIMEN_SOURCE_ID", StringType(), True),
            StructField("SPECIMEN_SOURCE_VALUE", StringType(), True),
            StructField("UNIT_SOURCE_VALUE", StringType(), True),
            StructField("ANATOMIC_SITE_SOURCE_VALUE", StringType(), True),
            StructField("DISEASE_STATUS_SOURCE_VALUE", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
