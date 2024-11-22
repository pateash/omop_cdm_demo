from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def reformat(spark: SparkSession, condition_occurence_1: DataFrame) -> DataFrame:
    return condition_occurence_1.select(
        col("CONDITION_OCCURRENCE_ID"), 
        col("person_id"), 
        col("CONDITION_CONCEPT_ID"), 
        col("CONDITION_START_DATETIME"), 
        coalesce(col("CONDITION_END_DATETIME"), date_add(col("CONDITION_START_DATETIME"), 1))\
          .alias("CONDITION_END_DATETIME")
    )
