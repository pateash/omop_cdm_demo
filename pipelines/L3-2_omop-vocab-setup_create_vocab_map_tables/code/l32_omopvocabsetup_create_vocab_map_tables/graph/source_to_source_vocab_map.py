from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l32_omopvocabsetup_create_vocab_map_tables.config.ConfigStore import *
from l32_omopvocabsetup_create_vocab_map_tables.udfs.UDFs import *

def source_to_source_vocab_map(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .mode("overwrite")\
        .saveAsTable(f"`{Config.catalog_name}`.`{Config.database_name}`.`source_to_source_vocab_map`")
