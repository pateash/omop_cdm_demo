from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l62_drug_analysis_topdrugsprescribedinthepast5years.config.ConfigStore import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.udfs.UDFs import *

def by_drug_concept_id(spark: SparkSession, t: DataFrame, c: DataFrame, ) -> DataFrame:
    return t\
        .alias("t")\
        .join(c.alias("c"), (col("c.CONCEPT_ID") == col("t.DRUG_CONCEPT_ID")), "inner")\
        .select(expr("extract('year', t.drug_era_start_datetime)").alias("year_of_era"), col("t.DRUG_CONCEPT_ID").alias("DRUG_CONCEPT_ID"), col("c.CONCEPT_NAME").alias("DRUG_CONCEPT_NAME"))
