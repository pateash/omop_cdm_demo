from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l6_druganalysis.config.ConfigStore import *
from l6_druganalysis.udfs.UDFs import *

def by_age_year_drug(spark: SparkSession, drug_count_demographics_projection: DataFrame) -> DataFrame:
    return drug_count_demographics_projection.orderBy(
        col("age_band").asc(), 
        col("year_of_era").asc(), 
        col("drug_concept_id").asc()
    )
