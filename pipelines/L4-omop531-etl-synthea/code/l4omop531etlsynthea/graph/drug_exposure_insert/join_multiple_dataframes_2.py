from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def join_multiple_dataframes_2(
        spark: SparkSession,
        c: DataFrame,
        srctostdvm: DataFrame,
        srctosrcvm: DataFrame, 
        fv: DataFrame, 
        p: DataFrame
) -> DataFrame:
    return c\
        .alias("c")\
        .join(
          srctostdvm.alias("srctostdvm"),
          (
            (((col("srctostdvm.SOURCE_CODE") == col("c.CODE")) & (col("srctostdvm.TARGET_DOMAIN_ID") == lit("Drug"))) & (col("srctostdvm.SOURCE_VOCABULARY_ID") == lit("RxNorm")))
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
          ((col("srctosrcvm.SOURCE_CODE") == col("c.CODE")) & (col("srctosrcvm.SOURCE_VOCABULARY_ID") == lit("RxNorm"))),
          "left_outer"
        )\
        .join(fv.alias("fv"), (col("fv.encounter_id") == col("c.ENCOUNTER")), "left_outer")\
        .join(p.alias("p"), (col("p.PERSON_SOURCE_VALUE") == col("c.PATIENT")), "inner")\
        .select(col("p.person_id").alias("person_id"), coalesce(col("srctostdvm.TARGET_CONCEPT_ID"), lit(0)).alias("drug_concept_id"), col("c.START").cast(DateType()).alias("drug_exposure_start_date"), col("c.START").cast(TimestampType()).alias("drug_exposure_start_datetime"), coalesce(col("c.STOP"), col("c.START")).cast(DateType()).alias("drug_exposure_end_date"), coalesce(col("c.STOP"), col("c.START")).cast(TimestampType()).alias("drug_exposure_end_datetime"), col("c.STOP").alias("verbatim_end_date"), lit(581452).alias("drug_type_concept_id"), lit(None).cast(StringType()).alias("stop_reason"), lit(0).alias("refills"), lit(0).alias("quantity"), coalesce(datediff(col("c.STOP"), col("c.START"))).alias("days_supply"), lit(None).cast(StringType()).alias("sig"), lit(0).alias("route_concept_id"), lit(0).alias("lot_number"), lit(0).alias("provider_id"), col("fv.VISIT_OCCURRENCE_ID_NEW").alias("visit_occurrence_id"), lit(0).alias("visit_detail_id"), col("c.CODE").alias("drug_source_value"), coalesce(col("srctosrcvm.SOURCE_CONCEPT_ID"), lit(0)).alias("drug_source_concept_id"), lit(None).alias("route_source_value"), lit(None).alias("dose_unit_source_value"))
