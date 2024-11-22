from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def encounters_projection(spark: SparkSession, encounters_by_patient: DataFrame) -> DataFrame:
    return encounters_by_patient.select(
        col("encounter_id"), 
        col("patient"), 
        col("encounterclass"), 
        col("VISIT_START_DATE"), 
        col("VISIT_END_DATE")
    )
