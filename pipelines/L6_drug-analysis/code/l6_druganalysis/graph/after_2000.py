from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l6_druganalysis.config.ConfigStore import *
from l6_druganalysis.udfs.UDFs import *

def after_2000(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("year_of_era") > lit(2000)))
