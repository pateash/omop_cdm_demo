from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def by_person_id_and_condition_concept_id(spark: SparkSession, c: DataFrame, e: DataFrame, ) -> DataFrame:
    return c\
        .alias("c")\
        .join(
          e.alias("e"),
          (
            ((col("c.person_id") == col("e.person_id")) & (col("c.CONDITION_CONCEPT_ID") == col("e.CONDITION_CONCEPT_ID")))
            & (col("e.end_date") >= col("c.CONDITION_START_DATETIME"))
          ),
          "inner"
        )\
        .select(col("c.CONDITION_OCCURRENCE_ID").alias("CONDITION_OCCURRENCE_ID"), col("c.person_id").alias("person_id"), col("c.CONDITION_CONCEPT_ID").alias("CONDITION_CONCEPT_ID"), col("c.CONDITION_START_DATETIME").alias("CONDITION_START_DATETIME"), col("c.CONDITION_END_DATETIME").alias("CONDITION_END_DATETIME"), col("e.end_date").alias("end_date"))
