from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l64_pairwiseassociation.config.ConfigStore import *
from l64_pairwiseassociation.udfs.UDFs import *

def drugs_and_names(spark: SparkSession, drug_era: DataFrame, c: DataFrame, ) -> DataFrame:
    return drug_era\
        .alias("drug_era")\
        .join(c.alias("c"), (col("drug_era.DRUG_CONCEPT_ID") == col("c.CONCEPT_ID")), "inner")\
        .select(col("drug_era.PERSON_ID").alias("PERSON_ID"), col("c.CONCEPT_NAME").alias("DRUG_CONCEPT_NAME"), col("drug_era.DRUG_CONCEPT_ID").alias("DRUG_CONCEPT_ID"), col("drug_era.drug_era_start_datetime").alias("DRUG_ERA_START_DATE"), col("drug_era.drug_era_end_datetime").alias("DRUG_ERA_END_DATE"))
