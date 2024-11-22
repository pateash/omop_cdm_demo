from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def ER_VISITS(spark: SparkSession, subgraph_config: SubgraphConfig) -> DataFrame:
    Config.update(subgraph_config)
    df_encounters_3 = encounters_3(spark)
    df_encounters_4 = encounters_4(spark)
    df_emergency_urgent_encounters = emergency_urgent_encounters(spark, df_encounters_3, df_encounters_4)
    df_encounters_by_patient = encounters_by_patient(spark, df_emergency_urgent_encounters)
    df_encounters_projection = encounters_projection(spark, df_encounters_by_patient)
    subgraph_config.update(Config)

    return df_encounters_projection
