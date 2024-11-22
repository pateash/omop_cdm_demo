from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def inpatient_encounters_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("ENCOUNTERCLASS") == lit("inpatient")))
