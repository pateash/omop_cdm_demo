from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def e(spark: SparkSession, RAWDATA_1: DataFrame) -> DataFrame:
    return RAWDATA_1.select(
        col("person_id"), 
        col("CONDITION_CONCEPT_ID"), 
        col("event_date"), 
        col("event_type"), 
        max(col("start_ordinal"))\
          .over(
            Window\
              .partitionBy(col("person_id"), col("CONDITION_CONCEPT_ID"))\
              .orderBy(col("event_date").asc(), col("event_type").asc())
          )\
          .alias("start_ordinal"), 
        row_number()\
          .over(
            Window\
              .partitionBy(col("person_id"), col("CONDITION_CONCEPT_ID"))\
              .orderBy(col("event_date").asc(), col("event_type").asc())
          )\
          .alias("overall_ord")
    )
