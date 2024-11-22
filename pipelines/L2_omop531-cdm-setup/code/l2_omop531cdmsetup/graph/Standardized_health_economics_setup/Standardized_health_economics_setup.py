from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l2_omop531cdmsetup.udfs.UDFs import *
from . import *
from .config import *

def Standardized_health_economics_setup(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_payer_plan_period_schema = payer_plan_period_schema(spark)
    df_cost_schema = cost_schema(spark)
    payer_plan_period(spark, df_payer_plan_period_schema)
    cost(spark, df_cost_schema)
    subgraph_config.update(Config)
