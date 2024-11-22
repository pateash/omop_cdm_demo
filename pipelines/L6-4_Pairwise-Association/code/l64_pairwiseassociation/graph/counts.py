from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l64_pairwiseassociation.config.ConfigStore import *
from l64_pairwiseassociation.udfs.UDFs import *

def counts(spark: SparkSession, drug_era_overlap: DataFrame) -> DataFrame:
    df1 = drug_era_overlap.groupBy(col("drug1"), col("drug2"))

    return df1.agg(count(lit(1)).alias("count_d1d2"))
