from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def latest_ingredient_events(spark: SparkSession, SetOperation_4: DataFrame) -> DataFrame:
    return SetOperation_4.select(
        col("person_id"), 
        col("ingredient_concept_id"), 
        col("event_date"), 
        col("event_type"), 
        max(col("start_ordinal"))\
          .over(
            Window\
              .partitionBy(col("person_id"), col("ingredient_concept_id"))\
              .orderBy(col("event_date").asc(), col("event_type").asc())
          )\
          .alias("start_ordinal"), 
        row_number()\
          .over(
            Window\
              .partitionBy(col("person_id"), col("ingredient_concept_id"))\
              .orderBy(col("event_date").asc(), col("event_type").asc())
          )\
          .alias("overall_ord")
    )
