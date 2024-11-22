from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l64_pairwiseassociation.config.ConfigStore import *
from l64_pairwiseassociation.udfs.UDFs import *

def pairwise_sums(spark: SparkSession, by_count_d1d2_asc: DataFrame) -> DataFrame:
    return by_count_d1d2_asc.select(
        col("drug1"), 
        col("drug2"), 
        col("count_d1d2"), 
        sum(col("count_d1d2")).over(Window.partitionBy(col("drug1"))).alias("sum_d1"), 
        sum(col("count_d1d2")).over(Window.partitionBy(col("drug2"))).alias("sum_d2"), 
        sum(col("count_d1d2")).over(Window.partitionBy(lit(1))).alias("sum_all")
    )
