from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def condition_start_ordinal(spark: SparkSession, cteConditionTarget: DataFrame) -> DataFrame:
    return cteConditionTarget.select(
        col("person_id"), 
        col("CONDITION_CONCEPT_ID"), 
        col("CONDITION_START_DATETIME").alias("event_date"), 
        lit(-1).alias("event_type"), 
        row_number()\
          .over(
            Window\
              .partitionBy(col("person_id"), col("CONDITION_CONCEPT_ID"))\
              .orderBy(col("CONDITION_START_DATETIME").asc())
          )\
          .alias("start_ordinal")
    )
