from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def condition_events_projection(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("person_id"), 
        col("CONDITION_CONCEPT_ID"), 
        date_add(col("CONDITION_END_DATETIME"), 30).alias("event_date"), 
        lit(1).alias("event_type"), 
        lit(None).cast(IntegerType()).alias("start_ordinal")
    )
