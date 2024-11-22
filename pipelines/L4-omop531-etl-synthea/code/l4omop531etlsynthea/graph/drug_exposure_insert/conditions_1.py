from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def conditions_1(spark: SparkSession) -> DataFrame:
    return spark.read.table("`omop`.`l0_bronze_layer`.`conditions`")