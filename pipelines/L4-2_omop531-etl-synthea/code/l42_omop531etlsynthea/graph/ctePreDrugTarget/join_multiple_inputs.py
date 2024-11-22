from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def join_multiple_inputs(spark: SparkSession, d: DataFrame, ca: DataFrame, c: DataFrame) -> DataFrame:
    return d\
        .alias("d")\
        .join(ca.alias("ca"), (col("ca.DESCENDANT_CONCEPT_ID") == col("d.drug_concept_id")), "inner")\
        .join(c.alias("c"), (col("ca.ANCESTOR_CONCEPT_ID") == col("c.CONCEPT_ID")), "inner")\
        .where(
          (
            ((col("c.VOCABULARY_ID") == lit("RxNorm")) & (col("c.CONCEPT_CLASS_ID") == lit("Ingredient")))
            & ((col("d.drug_concept_id") != lit(0)) & (coalesce(col("d.days_supply"), lit(0)) >= lit(0)))
          )
        )\
        .select(col("d.drug_exposure_id").alias("drug_exposure_id"), col("d.person_id").alias("person_id"), col("c.CONCEPT_ID").alias("ingredient_concept_id"), col("d.drug_exposure_start_datetime").alias("drug_exposure_start_datetime"), col("d.days_supply").alias("days_supply"), coalesce(
          expr("ifnull(d.drug_exposure_end_datetime, NULL)"), 
          expr(
            "ifnull(date_add(d.drug_exposure_start_datetime, CAST(d.days_supply AS INT)), d.drug_exposure_start_datetime)"
          ), 
          date_add(col("d.drug_exposure_start_datetime"), 1)
        )\
        .alias("drug_exposure_end_datetime"))
