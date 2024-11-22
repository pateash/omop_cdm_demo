from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l64_pairwiseassociation.config.ConfigStore import *
from l64_pairwiseassociation.udfs.UDFs import *

def by_count_d1d2_asc(spark: SparkSession, counts: DataFrame) -> DataFrame:
    return counts.orderBy(col("count_d1d2").desc())
