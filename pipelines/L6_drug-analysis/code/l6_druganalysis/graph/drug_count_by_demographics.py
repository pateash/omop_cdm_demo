from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l6_druganalysis.config.ConfigStore import *
from l6_druganalysis.udfs.UDFs import *

def drug_count_by_demographics(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("age_band"), 
        col("year_of_era"), 
        col("gender"), 
        col("drug_concept_name"), 
        col("drug_concept_id")
    )

    return df1.agg(count(lit(1)).alias("s_count"))
