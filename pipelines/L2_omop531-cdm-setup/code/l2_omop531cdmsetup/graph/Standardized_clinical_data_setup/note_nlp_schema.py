from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def note_nlp_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("NOTE_NLP_ID", LongType(), True),
            StructField("NOTE_ID", LongType(), True),
            StructField("SECTION_CONCEPT_ID", LongType(), True),
            StructField("SNIPPET", StringType(), True),
            StructField("OFFSET", StringType(), True),
            StructField("LEXICAL_VARIANT", StringType(), True),
            StructField("NOTE_NLP_CONCEPT_ID", LongType(), True),
            StructField("NOTE_NLP_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("NLP_SYSTEM", StringType(), True),
            StructField("NLP_DATE", DateType(), True),
            StructField("NLP_DATETIME", TimestampType(), True),
            StructField("TERM_EXISTS", StringType(), True),
            StructField("TERM_TEMPORAL", StringType(), True),
            StructField("TERM_MODIFIERS", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
