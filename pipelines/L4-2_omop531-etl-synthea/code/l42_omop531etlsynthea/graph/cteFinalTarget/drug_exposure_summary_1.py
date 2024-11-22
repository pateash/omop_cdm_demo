from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def drug_exposure_summary_1(spark: SparkSession, cteSubExposures: DataFrame) -> DataFrame:
    return cteSubExposures.select(
        col("row_number"), 
        col("person_id"), 
        col("drug_concept_id"), 
        col("drug_sub_exposure_start_datetime"), 
        col("drug_sub_exposure_end_datetime"), 
        col("drug_exposure_count"), 
        datediff(col("drug_sub_exposure_end_datetime"), col("drug_sub_exposure_start_datetime")).alias("days_exposed")
    )
