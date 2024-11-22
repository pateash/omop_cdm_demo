from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l42_omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def cteConditionTarget(spark: SparkSession, subgraph_config: SubgraphConfig) -> DataFrame:
    Config.update(subgraph_config)
    df_condition_occurence_1 = condition_occurence_1(spark)
    df_reformat = reformat(spark, df_condition_occurence_1)
    subgraph_config.update(Config)

    return df_reformat
