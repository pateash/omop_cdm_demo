from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def E(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("PATIENT").alias("patient"), 
        col("ENCOUNTERCLASS").alias("encounterclass"), 
        col("EVENT_DATE"), 
        col("EVENT_TYPE"), 
        max(col("START_ORDINAL"))\
          .over(
            Window\
              .partitionBy(col("PATIENT"), col("ENCOUNTERCLASS"))\
              .orderBy(col("EVENT_DATE").asc(), col("EVENT_TYPE").asc())
          )\
          .alias("START_ORDINAL"), 
        row_number()\
          .over(
            Window\
              .partitionBy(col("PATIENT"), col("ENCOUNTERCLASS"))\
              .orderBy(col("EVENT_DATE").asc(), col("EVENT_TYPE").asc())
          )\
          .alias("OVERALL_ORD")
    )
