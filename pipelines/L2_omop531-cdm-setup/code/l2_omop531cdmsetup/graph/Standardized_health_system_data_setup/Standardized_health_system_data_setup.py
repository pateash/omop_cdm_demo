from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l2_omop531cdmsetup.udfs.UDFs import *
from . import *
from .config import *

def Standardized_health_system_data_setup(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_provider_schema = provider_schema(spark)
    df_location_schema = location_schema(spark)
    location(spark, df_location_schema)
    df_care_site_schema = care_site_schema(spark)
    provider(spark, df_provider_schema)
    care_site(spark, df_care_site_schema)
    subgraph_config.update(Config)
