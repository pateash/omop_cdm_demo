from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def person_insert(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_patients = patients(spark)
    df_filter_gender_not_null = filter_gender_not_null(spark, df_patients)
    df_reformat_data = reformat_data(spark, df_filter_gender_not_null)
    person(spark, df_reformat_data)
    subgraph_config.update(Config)
