from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l2_omop531cdmsetup.udfs.UDFs import *
from . import *
from .config import *

def Standardized_metadata_setup(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_cdm_source_schema = cdm_source_schema(spark)
    df_metadata_schema = metadata_schema(spark)
    cdm_source(spark, df_cdm_source_schema)
    metadata(spark, df_metadata_schema)
    subgraph_config.update(Config)
