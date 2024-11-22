from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def end_dates_by_event_date(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("patient"), col("encounterclass"), date_add(col("EVENT_DATE"), -1).alias("END_DATE"))
