from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def by_person_and_ingredient_concept_id(spark: SparkSession, dt: DataFrame, e: DataFrame, ) -> DataFrame:
    return dt\
        .alias("dt")\
        .join(
          e.alias("e"),
          (
            ((col("e.person_id") == col("dt.person_id")) & (col("dt.ingredient_concept_id") == col("e.ingredient_concept_id")))
            & (col("e.end_datetime") >= col("dt.drug_exposure_start_datetime"))
          ),
          "inner"
        )\
        .select(col("dt.drug_exposure_id").alias("drug_exposure_id"), col("dt.person_id").alias("person_id"), col("dt.ingredient_concept_id").alias("ingredient_concept_id"), col("dt.drug_exposure_start_datetime").alias("drug_exposure_start_datetime"), col("dt.days_supply").alias("days_supply"), col("dt.drug_exposure_end_datetime").alias("drug_exposure_end_datetime"), col("e.end_datetime").alias("end_datetime"))
