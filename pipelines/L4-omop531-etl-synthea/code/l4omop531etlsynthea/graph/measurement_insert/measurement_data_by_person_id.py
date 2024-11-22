from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def measurement_data_by_person_id(spark: SparkSession, Subgraph_1: DataFrame) -> DataFrame:
    return Subgraph_1.select(
        row_number().over(Window.partitionBy(lit(1)).orderBy(col("person_id").asc())).alias("measurement_id"), 
        col("person_id"), 
        col("measurement_concept_id"), 
        col("measurement_date"), 
        col("measurement_datetime"), 
        col("measurement_time"), 
        col("measurement_type_concept_id"), 
        col("operator_concept_id"), 
        col("value_as_number"), 
        col("value_as_concept_id"), 
        col("unit_concept_id"), 
        col("range_low"), 
        col("range_high"), 
        col("provider_id"), 
        col("visit_occurrence_id"), 
        col("visit_detail_id"), 
        col("measurement_source_value"), 
        col("measurement_source_concept_id"), 
        col("unit_source_value"), 
        col("value_source_value")
    )
