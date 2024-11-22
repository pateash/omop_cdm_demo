from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l42_omop531etlsynthea.config.ConfigStore import *
from l42_omop531etlsynthea.udfs.UDFs import *

def drug_era_summary_reformat(spark: SparkSession, drug_era_summary: DataFrame) -> DataFrame:
    return drug_era_summary.select(
        row_number().over(Window.partitionBy(lit(1)).orderBy(col("person_id").asc())).alias("drug_era_id"), 
        col("person_id"), 
        col("drug_concept_id"), 
        col("drug_era_start_datetime"), 
        col("era_end_datetime").alias("drug_era_end_datetime"), 
        col("drug_exposure_count"), 
        col("gap_days")
    )
