from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def drug_exposures_by_person_id(spark: SparkSession, SetOperation_3: DataFrame) -> DataFrame:
    return SetOperation_3.select(
        row_number().over(Window.partitionBy(lit(1)).orderBy(col("person_id").asc())).alias("drug_exposure_id"), 
        col("person_id"), 
        col("drug_concept_id"), 
        col("drug_exposure_start_date"), 
        col("drug_exposure_start_datetime"), 
        col("drug_exposure_end_date"), 
        col("drug_exposure_end_datetime"), 
        col("verbatim_end_date"), 
        col("drug_type_concept_id"), 
        col("stop_reason"), 
        col("refills"), 
        col("quantity"), 
        col("days_supply"), 
        col("sig"), 
        col("route_concept_id"), 
        col("lot_number"), 
        col("provider_id"), 
        col("visit_occurrence_id"), 
        col("visit_detail_id"), 
        col("drug_source_value"), 
        col("drug_source_concept_id"), 
        col("route_source_value"), 
        col("dose_unit_source_value")
    )
