from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def reformatted_inpatient_encounters(spark: SparkSession, inpatient_encounters_1: DataFrame) -> DataFrame:
    return inpatient_encounters_1.select(
        col("PATIENT").alias("patient"), 
        col("ENCOUNTERCLASS").alias("encounterclass"), 
        date_add(col("STOP"), 1).alias("EVENT_DATE"), 
        lit(1).alias("EVENT_TYPE"), 
        lit(None).alias("START_ORDINAL")
    )
