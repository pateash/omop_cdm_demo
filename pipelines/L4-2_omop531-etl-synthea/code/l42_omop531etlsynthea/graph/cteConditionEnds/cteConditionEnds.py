from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l42_omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def cteConditionEnds(spark: SparkSession, subgraph_config: SubgraphConfig, c: DataFrame, e0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_by_person_id_and_condition_concept_id = by_person_id_and_condition_concept_id(spark, c, e0)
    df_Aggregate_1 = Aggregate_1(spark, df_by_person_id_and_condition_concept_id)
    df_condition_era_projection = condition_era_projection(spark, df_Aggregate_1)
    subgraph_config.update(Config)

    return df_condition_era_projection
