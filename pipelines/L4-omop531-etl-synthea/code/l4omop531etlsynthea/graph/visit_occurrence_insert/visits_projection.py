from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def visits_projection(spark: SparkSession, join_multiple_dataframes: DataFrame) -> DataFrame:
    return join_multiple_dataframes.select(
        col("visit_occurrence_id").alias("VISIT_OCCURRENCE_ID"), 
        col("person_id"), 
        when((lower(col("encounterclass")) == lit("ambulatory")), lit(9202))\
          .when((lower(col("encounterclass")) == lit("emergency")), lit(9203))\
          .when((lower(col("encounterclass")) == lit("inpatient")), lit(9201))\
          .when((lower(col("encounterclass")) == lit("wellness")), lit(9202))\
          .when((lower(col("encounterclass")) == lit("urgentcare")), lit(9203))\
          .when((lower(col("encounterclass")) == lit("outpatient")), lit(9202))\
          .otherwise(lit(0))\
          .alias("VISIT_CONCEPT_ID"), 
        col("VISIT_START_DATE").cast(DateType()).alias("VISIT_START_DATE"), 
        col("VISIT_START_DATE").cast(TimestampType()).alias("VISIT_START_DATETIME"), 
        col("VISIT_END_DATE").cast(DateType()).alias("VISIT_END_DATE"), 
        col("VISIT_END_DATE").cast(TimestampType()).alias("VISIT_END_DATETIME"), 
        lit(44818517).alias("VISIT_TYPE_CONCEPT_ID"), 
        lit(0).alias("PROVIDER_ID"), 
        lit(None).cast(IntegerType()).alias("CARE_SITE_ID"), 
        col("encounter_id").alias("VISIT_SOURCE_VALUE"), 
        lit(0).alias("VISIT_SOURCE_CONCEPT_ID"), 
        lit(0).alias("ADMITTING_SOURCE_CONCEPT_ID"), 
        lit(None).cast(StringType()).alias("ADMITTING_SOURCE_VALUE"), 
        lit(0).alias("DISCHARGE_TO_CONCEPT_ID"), 
        lit(None).cast(StringType()).alias("DISCHARGE_TO_SOURCE_VALUE"), 
        expr("lag(visit_occurrence_id)")\
          .over(Window.partitionBy(col("person_id")).orderBy(col("VISIT_START_DATE").asc()))\
          .alias("PRECEDING_VISIT_OCCURRENCE_ID")
    )
