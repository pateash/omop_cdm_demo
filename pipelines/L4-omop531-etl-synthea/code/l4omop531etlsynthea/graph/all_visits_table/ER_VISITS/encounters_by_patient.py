from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def encounters_by_patient(spark: SparkSession, emergency_urgent_encounters: DataFrame) -> DataFrame:
    df1 = emergency_urgent_encounters.groupBy(col("patient"), col("encounterclass"), col("VISIT_START_DATE"))

    return df1.agg(min(col("encounter_id")).alias("encounter_id"), max(col("VISIT_END_DATE")).alias("VISIT_END_DATE"))
