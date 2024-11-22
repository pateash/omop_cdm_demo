from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l1_cdm_setup.udfs.UDFs import *

def source_path_1(spark: SparkSession) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).option("sep", ",").csv(Config.source_path)
