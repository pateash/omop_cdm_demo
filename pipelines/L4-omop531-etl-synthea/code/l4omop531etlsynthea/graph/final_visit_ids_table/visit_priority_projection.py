from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def visit_priority_projection(spark: SparkSession, assign_all_visit_ids_1: DataFrame) -> DataFrame:
    return assign_all_visit_ids_1.select(
        col("encounter_id"), 
        col("person_source_value"), 
        col("date_service"), 
        col("date_service_end"), 
        col("encounterclass"), 
        col("VISIT_TYPE"), 
        col("VISIT_START_DATE"), 
        col("VISIT_END_DATE"), 
        col("VISIT_OCCURRENCE_ID"), 
        col("VISIT_OCCURRENCE_ID_NEW"), 
        when(
            col("encounterclass").isin(lit("emergency"), lit("urgent")), 
            when(((col("VISIT_TYPE") == lit("inpatient")) & col("VISIT_OCCURRENCE_ID_NEW").isNotNull()), lit(1))\
              .when(
                (col("VISIT_TYPE").isin(lit("emergency"), lit("urgent")) & col("VISIT_OCCURRENCE_ID_NEW").isNotNull()), 
                lit(2)
              )\
              .otherwise(lit(99))
          )\
          .alias("PRIORITY")
    )
