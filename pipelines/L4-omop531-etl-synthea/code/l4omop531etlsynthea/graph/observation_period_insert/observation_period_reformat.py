from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def observation_period_reformat(spark: SparkSession, by_person_start_end_dates: DataFrame) -> DataFrame:
    return by_person_start_end_dates.select(
        row_number().over(Window.partitionBy(lit(1)).orderBy(col("person_id").asc())).alias("OBSERVATION_PERIOD_ID"), 
        col("person_id"), 
        col("start_date").alias("OBSERVATION_PERIOD_START_DATE"), 
        col("end_date").alias("OBSERVATION_PERIOD_END_DATE"), 
        lit(44814724).alias("period_type_concept_id")
    )
