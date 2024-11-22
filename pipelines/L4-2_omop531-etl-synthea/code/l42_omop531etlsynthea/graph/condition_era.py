from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l42_omop531etlsynthea.config.ConfigStore import *
from l42_omop531etlsynthea.udfs.UDFs import *

def condition_era(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("overwriteSchema", True)\
        .mode("overwrite")\
        .saveAsTable(f"`{Config.catalog_name}`.`{Config.database_name}`.`condition_era`")
