from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l64_pairwiseassociation.config.ConfigStore import *
from l64_pairwiseassociation.udfs.UDFs import *

def COPRESCRIBED_1(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .mode("overwrite")\
        .saveAsTable(f"`{Config.catalog_name}`.`l1_silver_omop531results`.`COPRESCRIBED`")
