from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l2_omop531cdmsetup.udfs.UDFs import *
from . import *
from .config import *

def Standardized_derived_elements_setup(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_condition_era_schema = condition_era_schema(spark)
    condition_era(spark, df_condition_era_schema)
    df_drug_era_schema = drug_era_schema(spark)
    df_dose_era_schema = dose_era_schema(spark)
    dose_era(spark, df_dose_era_schema)
    drug_era(spark, df_drug_era_schema)
    subgraph_config.update(Config)
