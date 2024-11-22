from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def drug_exposure_summary_reformatted(spark: SparkSession, drug_exposure_summary: DataFrame) -> DataFrame:
    return drug_exposure_summary.select(
        row_number()\
          .over(
            Window\
              .partitionBy(col("person_id"), col("drug_concept_id"), col("drug_sub_exposure_end_datetime"))\
              .orderBy(col("person_id").asc())
          )\
          .alias("row_number"), 
        col("person_id"), 
        col("drug_concept_id"), 
        col("drug_sub_exposure_start_datetime"), 
        col("drug_sub_exposure_end_datetime"), 
        col("drug_exposure_count")
    )
