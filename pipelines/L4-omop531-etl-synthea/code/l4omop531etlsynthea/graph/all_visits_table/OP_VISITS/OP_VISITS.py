from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def OP_VISITS(spark: SparkSession, subgraph_config: SubgraphConfig) -> DataFrame:
    Config.update(subgraph_config)
    df_encounters_5 = encounters_5(spark)
    df_filter_encounterclass = filter_encounterclass(spark, df_encounters_5)
    df_encounter_id_by_patient = encounter_id_by_patient(spark, df_filter_encounterclass)
    df_CTE_VISITS_DISTINCT = CTE_VISITS_DISTINCT(spark, df_encounter_id_by_patient)
    df_encounters_by_patient_visit = encounters_by_patient_visit(spark, df_CTE_VISITS_DISTINCT)
    df_encounters_projection = encounters_projection(spark, df_encounters_by_patient_visit)
    subgraph_config.update(Config)

    return df_encounters_projection
