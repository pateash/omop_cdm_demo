from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l6_druganalysis.config.ConfigStore import *
from l6_druganalysis.udfs.UDFs import *

def drug_count_demographics_projection(spark: SparkSession, drug_count_by_demographics: DataFrame) -> DataFrame:
    return drug_count_by_demographics.select(
        col("drug_concept_name"), 
        col("drug_concept_id"), 
        col("s_count"), 
        col("age_band"), 
        col("year_of_era"), 
        col("gender")
    )
