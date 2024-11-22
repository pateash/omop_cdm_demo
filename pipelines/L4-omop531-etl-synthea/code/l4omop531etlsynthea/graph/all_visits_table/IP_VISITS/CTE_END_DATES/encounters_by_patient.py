from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def encounters_by_patient(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("PATIENT"), 
        col("ENCOUNTERCLASS"), 
        col("START").alias("EVENT_DATE"), 
        lit(-1).alias("EVENT_TYPE"), 
        row_number()\
          .over(Window.partitionBy(col("PATIENT"), col("ENCOUNTERCLASS")).orderBy(col("START").asc(), col("STOP").asc()))\
          .alias("START_ORDINAL")
    )
