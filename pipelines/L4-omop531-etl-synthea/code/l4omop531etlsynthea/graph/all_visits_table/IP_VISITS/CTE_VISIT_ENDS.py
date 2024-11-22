from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def CTE_VISIT_ENDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("PATIENT"), col("ENCOUNTERCLASS"), col("START"))

    return df1.agg(min(col("Id")).alias("encounter_id"), min(col("END_DATE")).alias("VISIT_END_DATE"))
