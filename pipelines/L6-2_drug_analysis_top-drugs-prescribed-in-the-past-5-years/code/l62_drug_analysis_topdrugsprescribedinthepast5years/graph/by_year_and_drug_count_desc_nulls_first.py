from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l62_drug_analysis_topdrugsprescribedinthepast5years.config.ConfigStore import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.udfs.UDFs import *

def by_year_and_drug_count_desc_nulls_first(
        spark: SparkSession,
        drug_count_by_concept_and_year_projection: DataFrame
) -> DataFrame:
    return drug_count_by_concept_and_year_projection.orderBy(
        col("year_of_era").asc(), 
        col("drug_count").desc_nulls_first()
    )
