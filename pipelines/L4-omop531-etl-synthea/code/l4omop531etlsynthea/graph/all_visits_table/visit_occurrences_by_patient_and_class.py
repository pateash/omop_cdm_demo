from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def visit_occurrences_by_patient_and_class(spark: SparkSession, SetOperation_1: DataFrame) -> DataFrame:
    return SetOperation_1.select(
        col("encounter_id"), 
        col("patient"), 
        col("encounterclass"), 
        col("VISIT_START_DATE"), 
        col("VISIT_END_DATE"), 
        row_number().over(Window.partitionBy(lit(1)).orderBy(col("patient").asc())).alias("visit_occurrence_id")
    )
