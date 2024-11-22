from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from bronze_ingest.udfs.UDFs import *

def L0_target(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable(f"`omop`.`l0_bronze_layer`.`{Config.table_name}`")
