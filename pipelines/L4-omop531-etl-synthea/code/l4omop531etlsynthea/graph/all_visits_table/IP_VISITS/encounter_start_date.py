from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def encounter_start_date(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("encounter_id"), col("PATIENT"), col("ENCOUNTERCLASS"), col("VISIT_END_DATE"))

    return df1.agg(min(col("VISIT_END_DATE")).alias("VISIT_START_DATE"))
