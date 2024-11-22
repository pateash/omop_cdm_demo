from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def join_multiple_dataframes_3(
        spark: SparkSession,
        i: DataFrame,
        srctostdvm: DataFrame,
        srctosrcvm: DataFrame, 
        fv: DataFrame, 
        p: DataFrame
) -> DataFrame:
    return i\
        .alias("i")\
        .join(
          srctostdvm.alias("srctostdvm"),
          (
            (((col("srctostdvm.SOURCE_CODE") == col("i.CODE")) & (col("srctostdvm.TARGET_DOMAIN_ID") == lit("Drug"))) & (col("srctostdvm.SOURCE_VOCABULARY_ID") == lit("CVX")))
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
          ((col("srctosrcvm.SOURCE_CODE") == col("i.CODE")) & (col("srctosrcvm.SOURCE_VOCABULARY_ID") == lit("CVX"))),
          "left_outer"
        )\
        .join(fv.alias("fv"), (col("fv.encounter_id") == col("i.ENCOUNTER")), "left_outer")\
        .join(p.alias("p"), (col("p.PERSON_SOURCE_VALUE") == col("i.PATIENT")), "inner")\
        .select(col("p.person_id").alias("person_id"), coalesce(col("srctostdvm.target_concept_id"), lit(0)).alias("drug_concept_id"), col("i.date").cast(DateType()).alias("drug_exposure_start_date"), col("i.DATE").cast(TimestampType()).alias("drug_exposure_start_datetime"), col("i.date").cast(DateType()).alias("drug_exposure_end_date"), col("i.DATE").cast(TimestampType()).alias("drug_exposure_end_datetime"), col("i.DATE").alias("verbatim_end_date"), lit(581452).alias("drug_type_concept_id"), lit(None).cast(StringType()).alias("stop_reason"), lit(0).alias("refills"), lit(0).alias("quantity"), lit(0).alias("days_supply"), lit(None).cast(StringType()).alias("sig"), lit(0).alias("route_concept_id"), lit(0).alias("lot_number"), lit(0).alias("provider_id"), col("fv.VISIT_OCCURRENCE_ID_NEW").alias("visit_occurrence_id"), lit(0).alias("visit_detail_id"), col("i.CODE").alias("drug_source_value"), coalesce(col("srctosrcvm.source_concept_id"), lit(0)).alias("drug_source_concept_id"), lit(None).cast(StringType()).alias("route_source_value"), lit(None).cast(StringType()).alias("dose_unit_source_value"))
