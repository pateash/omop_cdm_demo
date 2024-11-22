from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def encounter_id_by_patient(spark: SparkSession, encounters_5: DataFrame) -> DataFrame:
    df1 = encounters_5.groupBy(col("PATIENT"), col("ENCOUNTERCLASS"), col("START"), col("STOP"))

    return df1.agg(min(col("Id")).alias("encounter_id"))
