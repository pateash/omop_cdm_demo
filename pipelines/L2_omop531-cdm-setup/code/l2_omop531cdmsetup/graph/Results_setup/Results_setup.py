from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l2_omop531cdmsetup.udfs.UDFs import *
from . import *
from .config import *

def Results_setup(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_cohort_schema = cohort_schema(spark)
    cohort(spark, df_cohort_schema)
    df_cohort_definition_schema = cohort_definition_schema(spark)
    df_cohort_attribute_schema = cohort_attribute_schema(spark)
    cohort_attribute(spark, df_cohort_attribute_schema)
    cohort_definition(spark, df_cohort_definition_schema)
    subgraph_config.update(Config)
