from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l32_omopvocabsetup_create_vocab_map_tables.config.ConfigStore import *
from l32_omopvocabsetup_create_vocab_map_tables.udfs.UDFs import *

def union_all(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    nonEmptyDf = [x for x in [in0, in1] if x is not None]
    res = nonEmptyDf[0]
    rest = nonEmptyDf[1:]

    for inDF in rest:
        res = res.unionAll(inDF)

    return res
