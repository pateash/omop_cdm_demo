from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l6_druganalysis.config.ConfigStore import *
from l6_druganalysis.udfs.UDFs import *

def join_multiple_dataframes(spark: SparkSession, t: DataFrame, p: DataFrame, c: DataFrame, c1: DataFrame) -> DataFrame:
    return t\
        .alias("t")\
        .join(p.alias("p"), (col("t.person_id") == col("p.PERSON_ID")), "inner")\
        .join(c.alias("c"), (col("c.CONCEPT_ID") == col("t.drug_concept_id")), "inner")\
        .join(
          c1.alias("c1"),
          ((col("c1.CONCEPT_ID") == col("p.GENDER_CONCEPT_ID")) & (col("c.CONCEPT_ID") == lit("1124957"))),
          "inner"
        )\
        .select(floor(((expr("extract('year', t.drug_era_start_datetime)") - col("p.YEAR_OF_BIRTH")) / lit(10)))\
        .alias("age_band"), expr("extract('year', t.drug_era_start_datetime)").alias("year_of_era"), col("p.GENDER_CONCEPT_ID").alias("GENDER_CONCEPT_ID"), col("t.drug_concept_id").alias("drug_concept_id"), col("c.CONCEPT_NAME").alias("drug_concept_name"), col("c1.CONCEPT_NAME").alias("gender"))
