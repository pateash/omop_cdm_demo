from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def join_multiple_dataframes(
        spark: SparkSession,
        o: DataFrame,
        srctostdvm: DataFrame,
        srcmap1: DataFrame, 
        srcmap2: DataFrame, 
        srctosrcvm: DataFrame, 
        fv: DataFrame, 
        p: DataFrame
) -> DataFrame:
    return o\
        .alias("o")\
        .join(
          srctostdvm.alias("srctostdvm"),
          (
            (((col("srctostdvm.SOURCE_CODE") == col("o.CODE")) & (col("srctostdvm.TARGET_DOMAIN_ID") == lit("Measurement"))) & (col("srctostdvm.SOURCE_VOCABULARY_ID") == lit("LOINC")))
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
          srcmap1.alias("srcmap1"),
          (
            ((col("srcmap1.SOURCE_CODE") == col("o.UNITS")) & (col("srcmap1.TARGET_VOCABULARY_ID") == lit("UCUM")))
            & (
              (col("srcmap1.TARGET_STANDARD_CONCEPT") == lit("S"))
              & (col("srcmap1.TARGET_INVALID_REASON").isNull() | (col("srcmap1.TARGET_INVALID_REASON") == lit("")))
            )
          ),
          "left_outer"
        )\
        .join(
          srcmap2.alias("srcmap2"),
          (
            ((col("srcmap2.SOURCE_CODE") == col("o.VALUE")) & (col("srcmap2.TARGET_DOMAIN_ID") == lit("Meas value")))
            & (
              (col("srcmap2.TARGET_STANDARD_CONCEPT") == lit("S"))
              & (col("srcmap2.TARGET_INVALID_REASON").isNull() | (col("srcmap2.TARGET_INVALID_REASON") == lit("")))
            )
          ),
          "left_outer"
        )\
        .join(
          srctosrcvm.alias("srctosrcvm"),
          ((col("srctosrcvm.SOURCE_CODE") == col("o.CODE")) & (col("srctosrcvm.SOURCE_VOCABULARY_ID") == lit("LOINC"))),
          "left_outer"
        )\
        .join(fv.alias("fv"), (col("fv.encounter_id") == col("o.ENCOUNTER")), "left_outer")\
        .join(p.alias("p"), (col("p.PERSON_SOURCE_VALUE") == col("o.PATIENT")), "inner")\
        .select(col("p.person_id").alias("person_id"), coalesce(col("srctostdvm.TARGET_CONCEPT_ID"), lit(0)).alias("measurement_concept_id"), col("o.DATE").cast(DateType()).alias("measurement_date"), col("o.DATE").cast(TimestampType()).alias("measurement_datetime"), col("o.DATE").cast(StringType()).alias("measurement_time"), lit(5001).alias("MEASUREMENT_TYPE_CONCEPT_ID"), lit(0).alias("OPERATOR_CONCEPT_ID"), nanvl(col("o.VALUE").cast(FloatType()), lit(None)).alias("value_as_number"), coalesce(col("srcmap2.target_concept_id"), lit(0)).alias("value_as_concept_id"), coalesce(col("srcmap1.TARGET_CONCEPT_ID"), lit(0)).alias("unit_concept_id"), lit(None).cast(IntegerType()).alias("RANGE_LOW"), lit(None).cast(IntegerType()).alias("RANGE_HIGH"), lit(0).alias("provider_id"), col("fv.VISIT_OCCURRENCE_ID_NEW").alias("visit_occurrence_id"), lit(0).alias("visit_detail_id"), col("o.CODE").alias("measurement_source_value"), coalesce(col("srctosrcvm.SOURCE_CONCEPT_ID"), lit(0)).alias("measurement_source_concept_id"), col("o.UNITS").alias("unit_source_value"), col("o.VALUE").alias("value_source_value"))
