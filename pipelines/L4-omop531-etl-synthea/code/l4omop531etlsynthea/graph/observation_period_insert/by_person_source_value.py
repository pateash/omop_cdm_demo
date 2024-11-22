from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def by_person_source_value(spark: SparkSession, p: DataFrame, e: DataFrame, ) -> DataFrame:
    return p.alias("p").join(e.alias("e"), (col("p.PERSON_SOURCE_VALUE") == col("e.PATIENT")), "inner")
