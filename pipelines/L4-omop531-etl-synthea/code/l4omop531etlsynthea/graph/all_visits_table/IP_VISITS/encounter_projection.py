from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def encounter_projection(spark: SparkSession, encounter_start_date: DataFrame) -> DataFrame:
    return encounter_start_date.select(
        col("encounter_id"), 
        col("PATIENT").alias("patient"), 
        col("ENCOUNTERCLASS").alias("encounterclass"), 
        col("VISIT_START_DATE"), 
        col("VISIT_END_DATE")
    )
