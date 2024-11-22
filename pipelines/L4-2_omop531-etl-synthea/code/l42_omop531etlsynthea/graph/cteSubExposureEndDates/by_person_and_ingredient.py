from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def by_person_and_ingredient(spark: SparkSession, filter_by_ordinal_difference: DataFrame) -> DataFrame:
    return filter_by_ordinal_difference.select(
        col("person_id"), 
        col("ingredient_concept_id"), 
        col("event_date").alias("end_datetime")
    )
