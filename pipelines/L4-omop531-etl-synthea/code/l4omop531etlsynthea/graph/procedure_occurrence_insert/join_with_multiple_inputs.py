from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def join_with_multiple_inputs(
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
            (((col("srctostdvm.SOURCE_CODE") == col("pr.CODE")) & (col("srctostdvm.TARGET_DOMAIN_ID") == lit("Procedure"))) & (col("srctostdvm.SOURCE_VOCABULARY_ID") == lit("SNOMED")))
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
        .select(row_number()\
        .over(Window.partitionBy(lit(1)).orderBy(col("p.person_id").asc()))\
        .alias("PROCEDURE_OCCURRENCE_ID"), col("p.person_id").alias("PERSON_ID"), coalesce(col("srctostdvm.TARGET_CONCEPT_ID"), lit(0)).alias("PROCEDURE_CONCEPT_ID"), col("pr.DATE").cast(DateType()).alias("PROCEDURE_DATE"), col("pr.DATE").cast(TimestampType()).alias("PROCEDURE_DATETIME"), lit(38000275).alias("PROCEDURE_TYPE_CONCEPT_ID"), lit(0).alias("MODIFIER_CONCEPT_ID"), lit(None).cast(IntegerType()).alias("QUANTITY"), lit(0).alias("PROVIDER_ID"), col("fv.VISIT_OCCURRENCE_ID_NEW").alias("VISIT_OCCURRENCE_ID"), lit(0).alias("VISIT_DETAIL_ID"), col("pr.CODE").alias("PROCEDURE_SOURCE_VALUE"), coalesce(col("srctosrcvm.SOURCE_CONCEPT_ID"), lit(0)).alias("PROCEDURE_SOURCE_CONCEPT_ID"), lit(None).cast(StringType()).alias("MODIFIER_SOURCE_VALUE"))
