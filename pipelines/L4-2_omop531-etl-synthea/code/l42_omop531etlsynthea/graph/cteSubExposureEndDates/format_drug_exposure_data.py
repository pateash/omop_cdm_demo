from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def format_drug_exposure_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("person_id"), 
        col("ingredient_concept_id"), 
        col("drug_exposure_start_datetime").alias("event_date"), 
        lit(1).alias("event_type"), 
        lit(None).cast(IntegerType()).alias("start_ordinal")
    )
