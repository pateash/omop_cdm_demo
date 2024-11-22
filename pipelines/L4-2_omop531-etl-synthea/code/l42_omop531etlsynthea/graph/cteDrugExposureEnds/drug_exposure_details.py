from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def drug_exposure_details(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("person_id"), 
        col("ingredient_concept_id").alias("drug_concept_id"), 
        col("drug_exposure_start_datetime"), 
        col("drug_sub_exposure_end_datetime")
    )
