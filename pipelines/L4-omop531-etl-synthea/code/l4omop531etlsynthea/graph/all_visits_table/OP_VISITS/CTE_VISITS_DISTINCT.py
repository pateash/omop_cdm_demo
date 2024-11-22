from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def CTE_VISITS_DISTINCT(spark: SparkSession, encounter_id_by_patient: DataFrame) -> DataFrame:
    return encounter_id_by_patient.select(
        col("encounter_id"), 
        col("PATIENT").alias("patient"), 
        col("ENCOUNTERCLASS").alias("encounterclass"), 
        col("START").alias("VISIT_START_DATE"), 
        col("STOP").alias("VISIT_END_DATE")
    )
