from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def encounters_with_priority(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("encounter_id"), 
        col("person_source_value"), 
        col("date_service"), 
        col("date_service_end"), 
        col("encounterclass"), 
        col("VISIT_TYPE"), 
        col("VISIT_START_DATE"), 
        col("VISIT_END_DATE"), 
        col("VISIT_OCCURRENCE_ID"), 
        col("VISIT_OCCURRENCE_ID_NEW"), 
        col("PRIORITY"), 
        row_number().over(Window.partitionBy(col("encounter_id")).orderBy(col("PRIORITY").asc())).alias("RN")
    )
