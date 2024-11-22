from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def by_person_condition_date(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("person_id"), col("CONDITION_CONCEPT_ID"), date_add(col("event_date"), -30).alias("end_date"))
