from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def all_visits_table(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_OP_VISITS = OP_VISITS(spark, subgraph_config.OP_VISITS)
    df_IP_VISITS = IP_VISITS(spark, subgraph_config.IP_VISITS)
    df_ER_VISITS = ER_VISITS(spark, subgraph_config.ER_VISITS)
    df_SetOperation_1 = SetOperation_1(spark, df_IP_VISITS, df_ER_VISITS, df_OP_VISITS)
    df_visit_occurrences_by_patient_and_class = visit_occurrences_by_patient_and_class(spark, df_SetOperation_1)
    all_visits(spark, df_visit_occurrences_by_patient_and_class)
    subgraph_config.update(Config)
