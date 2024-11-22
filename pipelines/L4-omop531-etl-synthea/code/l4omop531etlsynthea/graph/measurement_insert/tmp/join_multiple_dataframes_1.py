from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def join_multiple_dataframes_1(
        spark: SparkSession,
        pr: DataFrame,
        srctostdvm: DataFrame,
        srctosrcvm: DataFrame, 
        fv: DataFrame, 
        p: DataFrame
) -> DataFrame:
    return pr\
        .alias("pr")\
        .join(
          srctostdvm.alias("srctostdvm"),
          (
            (((col("srctostdvm.SOURCE_CODE") == col("pr.CODE")) & (col("srctostdvm.TARGET_DOMAIN_ID") == lit("Measurement"))) & (col("srctostdvm.SOURCE_VOCABULARY_ID") == lit("SNOMED")))
            & (
              (col("srctostdvm.TARGET_STANDARD_CONCEPT") == lit("S"))
              & (
                col("srctostdvm.TARGET_INVALID_REASON").isNull()
                | (col("srctostdvm.TARGET_INVALID_REASON") == lit(""))
              )
            )
          ),
          "inner"
        )\
        .join(
          srctosrcvm.alias("srctosrcvm"),
          (
            (col("srctosrcvm.SOURCE_CODE") == col("pr.CODE"))
            & (col("srctosrcvm.SOURCE_VOCABULARY_ID") == lit("SNOMED"))
          ),
          "left_outer"
        )\
        .join(fv.alias("fv"), (col("fv.encounter_id") == col("pr.ENCOUNTER")), "left_outer")\
        .join(p.alias("p"), (col("p.PERSON_SOURCE_VALUE") == col("pr.PATIENT")), "inner")\
        .select(col("p.person_id").alias("person_id"), coalesce(col("srctostdvm.TARGET_CONCEPT_ID"), lit(0)).alias("measurement_concept_id"), col("pr.DATE").cast(DateType()).alias("measurement_date"), col("pr.DATE").cast(TimestampType()).alias("measurement_datetime"), col("pr.date").cast(StringType()).alias("measurement_time"), lit(5001).alias("measurement_type_concept_id"), lit(0).alias("operator_concept_id"), lit(None).cast(IntegerType()).alias("value_as_number"), lit(0).alias("value_as_concept_id"), lit(0).alias("unit_concept_id"), lit(None).cast(IntegerType()).alias("range_low"), lit(None).cast(IntegerType()).alias("range_high"), lit(0).alias("provider_id"), col("fv.VISIT_OCCURRENCE_ID_NEW").alias("visit_occurrence_id"), lit(0).alias("visit_detail_id"), col("pr.CODE").alias("measurement_source_value"), coalesce(col("srctosrcvm.SOURCE_CONCEPT_ID"), lit(0)).alias("measurement_source_concept_id"), lit(None).cast(StringType()).alias("unit_source_value"), lit(None).cast(StringType()).alias("value_source_value"))
