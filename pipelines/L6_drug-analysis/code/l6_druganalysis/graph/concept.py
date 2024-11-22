from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l6_druganalysis.config.ConfigStore import *
from l6_druganalysis.udfs.UDFs import *

def concept(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.catalog_name}`.`{Config.database_name}`.`concept`")
