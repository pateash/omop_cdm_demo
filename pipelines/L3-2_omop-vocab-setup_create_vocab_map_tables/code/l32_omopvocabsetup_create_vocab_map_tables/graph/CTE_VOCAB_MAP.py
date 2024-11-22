from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l32_omopvocabsetup_create_vocab_map_tables.config.ConfigStore import *
from l32_omopvocabsetup_create_vocab_map_tables.udfs.UDFs import *

def CTE_VOCAB_MAP(spark: SparkSession, C: DataFrame, CR: DataFrame, C1: DataFrame) -> DataFrame:
    return C\
        .alias("C")\
        .join(
          CR.alias("CR"),
          (
            ((col("C.CONCEPT_ID") == col("CR.CONCEPT_ID_1")) & col("CR.INVALID_REASON").isNull())
            & (lower(col("CR.RELATIONSHIP_ID")) == lit("maps to"))
          ),
          "inner"
        )\
        .join(
          C1.alias("C1"),
          ((col("CR.CONCEPT_ID_2") == col("C1.CONCEPT_ID")) & col("C1.INVALID_REASON").isNull()),
          "inner"
        )\
        .select(col("C.CONCEPT_CODE").alias("SOURCE_CODE"), col("C.CONCEPT_ID").alias("SOURCE_CONCEPT_ID"), col("C.CONCEPT_NAME").alias("SOURCE_CODE_DESCRIPTION"), col("C.VOCABULARY_ID").alias("SOURCE_VOCABULARY_ID"), col("C.DOMAIN_ID").alias("SOURCE_DOMAIN_ID"), col("C.CONCEPT_CLASS_ID").alias("SOURCE_CONCEPT_CLASS_ID"), col("C.VALID_START_DATE").alias("SOURCE_VALID_START_DATE"), col("C.VALID_END_DATE").alias("SOURCE_VALID_END_DATE"), col("C.INVALID_REASON").alias("SOURCE_INVALID_REASON"), col("C1.CONCEPT_ID").alias("TARGET_CONCEPT_ID"), col("C1.CONCEPT_NAME").alias("TARGET_CONCEPT_NAME"), col("C1.VOCABULARY_ID").alias("TARGET_VOCABULARY_ID"), col("C1.DOMAIN_ID").alias("TARGET_DOMAIN_ID"), col("C1.CONCEPT_CLASS_ID").alias("TARGET_CONCEPT_CLASS_ID"), col("C1.INVALID_REASON").alias("TARGET_INVALID_REASON"), col("C1.STANDARD_CONCEPT").alias("TARGET_STANDARD_CONCEPT"))
