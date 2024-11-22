from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def filter_gender_not_null(spark: SparkSession, patients: DataFrame) -> DataFrame:
    return patients.filter(col("GENDER").isNotNull())
