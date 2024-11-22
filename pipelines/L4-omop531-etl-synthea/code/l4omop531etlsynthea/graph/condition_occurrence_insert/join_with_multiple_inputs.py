from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def join_with_multiple_inputs(
        spark: SparkSession,
        c: DataFrame,
        srstostdvm: DataFrame,
        srctosrcvm: DataFrame, 
        fv: DataFrame, 
        p: DataFrame
) -> DataFrame:
    return c\
        .alias("c")\
        .join(
          srstostdvm.alias("srstostdvm"),
          (
            (((col("srstostdvm.SOURCE_CODE") == col("c.CODE")) & (col("srstostdvm.TARGET_DOMAIN_ID") == lit("Condition"))) & (col("srstostdvm.SOURCE_VOCABULARY_ID") == lit("SNOMED")))
            & (
              (col("srstostdvm.TARGET_STANDARD_CONCEPT") == lit("S"))
              & (
                col("srstostdvm.TARGET_INVALID_REASON").isNull()
                | (col("srstostdvm.TARGET_INVALID_REASON") == lit(""))
              )
            )
          ),
          "inner"
        )\
        .join(
          srctosrcvm.alias("srctosrcvm"),
          ((col("srctosrcvm.SOURCE_CODE") == col("c.CODE")) & (col("srctosrcvm.SOURCE_VOCABULARY_ID") == lit("SNOMED"))),
          "left_outer"
        )\
        .join(fv.alias("fv"), (col("fv.encounter_id") == col("c.ENCOUNTER")), "left_outer")\
        .join(p.alias("p"), (col("c.PATIENT") == col("p.PERSON_SOURCE_VALUE")), "inner")\
        .select(row_number()\
        .over(Window.partitionBy(lit(1)).orderBy(col("p.person_id").asc()))\
        .alias("CONDITION_OCCURRENCE_ID"), col("p.person_id").alias("person_id"), coalesce(col("srstostdvm.TARGET_CONCEPT_ID"), lit(0)).alias("target_concept_id"), col("c.START").cast(DateType()).alias("CONDITION_START_DATE"), col("c.START").cast(TimestampType()).alias("CONDITION_START_DATETIME"), col("c.STOP").cast(DateType()).alias("CONDITION_END_DATE"), col("c.STOP").cast(TimestampType()).alias("CONDITION_END_DATETIME"), lit(32020).alias("CONDITION_TYPE_CONCEPT_ID"), lit(None).cast(StringType()).alias("STOP_REASON"), lit(0).alias("PROVIDER_ID"), col("fv.VISIT_OCCURRENCE_ID_NEW").alias("VISIT_OCCURRENCE_ID"), lit(0).alias("VISIT_DETAIL_ID"), col("c.CODE").alias("CONDITION_SOURCE_VALUE"), coalesce(col("srctosrcvm.SOURCE_CONCEPT_ID"), lit(0)).alias("CONDITION_SOURCE_CONCEPT_ID"), lit(None).cast(StringType()).alias("CONDITION_STATUS_SOURCE_VALUE"), lit(0).alias("CONDITION_STATUS_CONCEPT_ID"))
