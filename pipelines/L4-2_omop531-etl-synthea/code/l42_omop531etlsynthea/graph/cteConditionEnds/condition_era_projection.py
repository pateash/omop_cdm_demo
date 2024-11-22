from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def condition_era_projection(spark: SparkSession, Aggregate_1: DataFrame) -> DataFrame:
    return Aggregate_1.select(
        col("person_id"), 
        col("CONDITION_CONCEPT_ID"), 
        col("CONDITION_START_DATETIME"), 
        col("era_end_datetime")
    )
