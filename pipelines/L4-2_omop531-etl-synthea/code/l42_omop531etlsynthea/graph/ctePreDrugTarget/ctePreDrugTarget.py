from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l42_omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def ctePreDrugTarget(spark: SparkSession, subgraph_config: SubgraphConfig) -> DataFrame:
    Config.update(subgraph_config)
    df_concept_ancestor = concept_ancestor(spark)
    df_drug_exposure_1 = drug_exposure_1(spark)
    df_concept = concept(spark)
    df_join_multiple_inputs = join_multiple_inputs(spark, df_drug_exposure_1, df_concept_ancestor, df_concept)
    subgraph_config.update(Config)

    return df_join_multiple_inputs
