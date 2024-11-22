from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def assign_all_visit_ids_table(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_all_visits_1 = all_visits_1(spark)
    df_encounters_6 = encounters_6(spark)
    df_join_multiple_inputs = join_multiple_inputs(spark, df_encounters_6, df_all_visits_1)
    assign_all_visit_ids(spark, df_join_multiple_inputs)
    subgraph_config.update(Config)
