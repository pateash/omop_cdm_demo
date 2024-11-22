from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l64_pairwiseassociation.config.ConfigStore import *
from l64_pairwiseassociation.udfs.UDFs import *

def COPRESCRIBED(spark: SparkSession, pairwise_sums: DataFrame) -> DataFrame:
    return pairwise_sums.select(
        col("drug1"), 
        col("drug2"), 
        col("count_d1d2"), 
        (col("count_d1d2") / col("sum_d1")).alias("supp_d1d2"), 
        (col("count_d1d2") / col("sum_d2")).alias("supp_d2d1"), 
        (col("sum_d1") / col("sum_all")).alias("supp_d1"), 
        (col("sum_d2") / col("sum_all")).alias("supp_d2")
    )
