from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l62_drug_analysis_topdrugsprescribedinthepast5years.config.ConfigStore import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.udfs.UDFs import *

def drug_count_by_concept_and_year_projection(
        spark: SparkSession,
        drug_count_by_concept_and_year: DataFrame
) -> DataFrame:
    return drug_count_by_concept_and_year.select(
        col("DRUG_CONCEPT_NAME"), 
        col("DRUG_CONCEPT_ID"), 
        col("year_of_era"), 
        col("drug_count")
    )
