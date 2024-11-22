from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l32_omopvocabsetup_create_vocab_map_tables.config.ConfigStore import *
from l32_omopvocabsetup_create_vocab_map_tables.udfs.UDFs import *

def concept_mapping(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("CONCEPT_CODE").alias("SOURCE_CODE"), 
        col("CONCEPT_ID").alias("SOURCE_CONCEPT_ID"), 
        col("CONCEPT_NAME").alias("SOURCE_CODE_DESCRIPTION"), 
        col("VOCABULARY_ID").alias("SOURCE_VOCABULARY_ID"), 
        col("DOMAIN_ID").alias("SOURCE_DOMAIN_ID"), 
        col("CONCEPT_CLASS_ID").alias("SOURCE_CONCEPT_CLASS_ID"), 
        col("VALID_START_DATE").alias("SOURCE_VALID_START_DATE"), 
        col("VALID_END_DATE").alias("SOURCE_VALID_END_DATE"), 
        col("INVALID_REASON").alias("SOURCE_INVALID_REASON"), 
        col("CONCEPT_ID").alias("TARGET_CONCEPT_ID"), 
        col("CONCEPT_NAME").alias("TARGET_CONCEPT_NAME"), 
        col("VOCABULARY_ID").alias("TARGET_VOCABULARY_ID"), 
        col("DOMAIN_ID").alias("TARGET_DOMAIN_ID"), 
        col("CONCEPT_CLASS_ID").alias("TARGET_CONCEPT_CLASS_ID"), 
        col("INVALID_REASON").alias("TARGET_INVALID_REASON"), 
        col("STANDARD_CONCEPT").alias("TARGET_STANDARD_CONCEPT")
    )
