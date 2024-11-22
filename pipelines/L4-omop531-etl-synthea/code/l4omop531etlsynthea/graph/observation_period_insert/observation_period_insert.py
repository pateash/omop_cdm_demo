from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def observation_period_insert(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_person_1 = person_1(spark)
    df_encounters_7 = encounters_7(spark)
    df_by_person_source_value = by_person_source_value(spark, df_person_1, df_encounters_7)
    df_by_person_start_end_dates = by_person_start_end_dates(spark, df_by_person_source_value)
    df_observation_period_reformat = observation_period_reformat(spark, df_by_person_start_end_dates)
    observation_period(spark, df_observation_period_reformat)
    subgraph_config.update(Config)
