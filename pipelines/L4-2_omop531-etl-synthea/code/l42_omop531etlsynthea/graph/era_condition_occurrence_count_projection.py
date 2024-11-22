from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l42_omop531etlsynthea.config.ConfigStore import *
from l42_omop531etlsynthea.udfs.UDFs import *

def era_condition_occurrence_count_projection(
        spark: SparkSession,
        era_condition_occurrence_count: DataFrame
) -> DataFrame:
    return era_condition_occurrence_count.select(
        row_number().over(Window.partitionBy(lit(1)).orderBy(col("person_id").asc())).alias("condition_era_id"), 
        col("person_id"), 
        col("CONDITION_CONCEPT_ID"), 
        col("condition_era_start_datetime"), 
        col("era_end_datetime").alias("condition_era_end_datetime"), 
        col("condition_occurrence_count")
    )
