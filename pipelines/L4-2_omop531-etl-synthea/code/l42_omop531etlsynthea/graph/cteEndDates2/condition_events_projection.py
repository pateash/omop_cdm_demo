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
        col("drug_concept_id"), 
        date_add(col("drug_sub_exposure_end_datetime"), 30).alias("event_date"), 
        lit(1).alias("event_type"), 
        lit(None).cast(IntegerType()).alias("start_ordinal")
    )
