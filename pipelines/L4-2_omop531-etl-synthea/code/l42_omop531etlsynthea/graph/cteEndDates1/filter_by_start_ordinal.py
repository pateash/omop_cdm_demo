from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def filter_by_start_ordinal(spark: SparkSession, e: DataFrame) -> DataFrame:
    return e.filter((((lit(2) * col("start_ordinal")) - col("overall_ord")) == lit(0)))
