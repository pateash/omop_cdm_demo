from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def emergency_urgent_encounters(spark: SparkSession, CL1: DataFrame, CL2: DataFrame, ) -> DataFrame:
    return CL1\
        .alias("CL1")\
        .join(
          CL2.alias("CL2"),
          (
            ((col("CL1.PATIENT") == col("CL2.PATIENT")) & (col("CL1.START") == col("CL2.START")))
            & (col("CL1.ENCOUNTERCLASS") == col("CL2.ENCOUNTERCLASS"))
          ),
          "inner"
        )\
        .where(col("CL1.ENCOUNTERCLASS").isin(lit("emergency"), lit("urgent")))\
        .select(col("CL1.Id").alias("encounter_id"), col("CL1.PATIENT").alias("patient"), col("CL1.ENCOUNTERCLASS").alias("encounterclass"), col("CL1.START").alias("VISIT_START_DATE"), col("CL2.STOP").alias("VISIT_END_DATE"))
