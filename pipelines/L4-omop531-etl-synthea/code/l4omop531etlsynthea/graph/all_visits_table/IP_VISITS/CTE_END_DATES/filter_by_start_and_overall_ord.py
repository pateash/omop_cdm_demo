from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def filter_by_start_and_overall_ord(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((((lit(2) * col("START_ORDINAL")) - col("OVERALL_ORD")) == lit(0)))
