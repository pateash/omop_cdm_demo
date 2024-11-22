from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def drug_era_summary(spark: SparkSession, era_end_datetime_by_person_and_drug: DataFrame) -> DataFrame:
    return era_end_datetime_by_person_and_drug.select(
        col("person_id"), 
        col("drug_concept_id"), 
        col("drug_sub_exposure_start_datetime"), 
        col("era_end_datetime"), 
        col("drug_exposure_count"), 
        col("days_exposed")
    )
