from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l32_omopvocabsetup_create_vocab_map_tables.config.ConfigStore import *
from l32_omopvocabsetup_create_vocab_map_tables.udfs.UDFs import *

def join_multiple_dataframes(spark: SparkSession, stcm: DataFrame, c1: DataFrame, c2: DataFrame) -> DataFrame:
    return stcm\
        .alias("stcm")\
        .join(c1.alias("c1"), (col("c1.CONCEPT_ID") == col("stcm.SOURCE_CONCEPT_ID")), "left_outer")\
        .join(c2.alias("c2"), (col("c2.CONCEPT_ID") == col("stcm.TARGET_CONCEPT_ID")), "left_outer")\
        .where(col("stcm.INVALID_REASON").isNull())\
        .select(col("stcm.SOURCE_CODE").alias("source_code"), col("stcm.SOURCE_CONCEPT_ID").alias("SOURCE_CONCEPT_ID"), col("stcm.SOURCE_CODE_DESCRIPTION").alias("SOURCE_CODE_DESCRIPTION"), col("stcm.SOURCE_VOCABULARY_ID").alias("source_vocabulary_id"), col("c1.DOMAIN_ID").alias("SOURCE_DOMAIN_ID"), col("c2.CONCEPT_CLASS_ID").alias("SOURCE_CONCEPT_CLASS_ID"), col("c1.VALID_START_DATE").alias("SOURCE_VALID_START_DATE"), col("c1.VALID_END_DATE").alias("SOURCE_VALID_END_DATE"), col("stcm.INVALID_REASON").alias("SOURCE_INVALID_REASON"), col("stcm.TARGET_CONCEPT_ID").alias("target_concept_id"), col("c2.CONCEPT_NAME").alias("TARGET_CONCEPT_NAME"), col("stcm.TARGET_VOCABULARY_ID").alias("target_vocabulary_id"), col("c2.DOMAIN_ID").alias("TARGET_DOMAIN_ID"), col("c2.CONCEPT_CLASS_ID").alias("TARGET_CONCEPT_CLASS_ID"), col("c2.INVALID_REASON").alias("TARGET_INVALID_REASON"), col("c2.STANDARD_CONCEPT").alias("TARGET_STANDARD_CONCEPT"))
