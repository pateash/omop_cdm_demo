from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def measurement_insert(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_tmp = tmp(spark, subgraph_config.tmp)
    df_measurement_data_by_person_id = measurement_data_by_person_id(spark, df_tmp)
    measurement(spark, df_measurement_data_by_person_id)
    subgraph_config.update(Config)
