from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def IP_VISITS(spark: SparkSession, subgraph_config: SubgraphConfig) -> DataFrame:
    Config.update(subgraph_config)
    df_CTE_END_DATES = CTE_END_DATES(spark, subgraph_config.CTE_END_DATES)
    df_encounters_2 = encounters_2(spark)
    df_encounters_and_visits_join = encounters_and_visits_join(spark, df_CTE_END_DATES, df_encounters_2)
    df_CTE_VISIT_ENDS = CTE_VISIT_ENDS(spark, df_encounters_and_visits_join)
    df_encounter_start_date = encounter_start_date(spark, df_CTE_VISIT_ENDS)
    df_encounter_projection = encounter_projection(spark, df_encounter_start_date)
    subgraph_config.update(Config)

    return df_encounter_projection
