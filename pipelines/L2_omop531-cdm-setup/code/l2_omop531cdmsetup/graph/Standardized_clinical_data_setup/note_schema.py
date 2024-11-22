from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def note_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("NOTE_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("NOTE_DATE", DateType(), True),
            StructField("NOTE_DATETIME", TimestampType(), True),
            StructField("NOTE_TYPE_CONCEPT_ID", LongType(), True),
            StructField("NOTE_CLASS_CONCEPT_ID", LongType(), True),
            StructField("NOTE_TITLE", StringType(), True),
            StructField("NOTE_TEXT", StringType(), True),
            StructField("ENCODING_CONCEPT_ID", LongType(), True),
            StructField("LANGUAGE_CONCEPT_ID", LongType(), True),
            StructField("PROVIDER_ID", LongType(), True),
            StructField("VISIT_OCCURRENCE_ID", LongType(), True),
            StructField("VISIT_DETAIL_ID", LongType(), True),
            StructField("NOTE_SOURCE_VALUE", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
