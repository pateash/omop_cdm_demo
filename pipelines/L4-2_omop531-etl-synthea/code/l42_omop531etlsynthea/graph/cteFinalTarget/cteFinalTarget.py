from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l42_omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def cteFinalTarget(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_drug_exposure_summary_1 = drug_exposure_summary_1(spark, in0)
    subgraph_config.update(Config)

    return df_drug_exposure_summary_1
