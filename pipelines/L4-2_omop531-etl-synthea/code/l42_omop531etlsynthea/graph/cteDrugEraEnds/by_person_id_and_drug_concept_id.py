from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def by_person_id_and_drug_concept_id(spark: SparkSession, ft: DataFrame, e: DataFrame, ) -> DataFrame:
    return ft\
        .alias("ft")\
        .join(
          e.alias("e"),
          (
            ((col("e.person_id") == col("ft.person_id")) & (col("ft.drug_concept_id") == col("e.DRUG_CONCEPT_ID")))
            & (col("e.end_datetime") >= col("ft.drug_sub_exposure_start_datetime"))
          ),
          "inner"
        )\
        .select(col("ft.row_number").alias("row_number"), col("ft.person_id").alias("person_id"), col("ft.drug_concept_id").alias("drug_concept_id"), col("ft.drug_sub_exposure_start_datetime").alias("drug_sub_exposure_start_datetime"), col("ft.drug_sub_exposure_end_datetime").alias("drug_sub_exposure_end_datetime"), col("ft.drug_exposure_count").alias("drug_exposure_count"), col("ft.days_exposed").alias("days_exposed"), col("e.end_datetime").alias("end_datetime"))
