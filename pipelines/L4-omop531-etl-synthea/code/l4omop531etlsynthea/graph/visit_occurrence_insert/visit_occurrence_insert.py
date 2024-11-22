from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def visit_occurrence_insert(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_person_2 = person_2(spark)
    df_final_visit_ids_1 = final_visit_ids_1(spark)
    df_by_visit_occurrence_id_new = by_visit_occurrence_id_new(spark, df_final_visit_ids_1)
    df_all_visits_2 = all_visits_2(spark)
    df_join_multiple_dataframes = join_multiple_dataframes(
        spark, 
        df_all_visits_2, 
        df_by_visit_occurrence_id_new, 
        df_person_2
    )
    df_visits_projection = visits_projection(spark, df_join_multiple_dataframes)
    visit_occurence(spark, df_visits_projection)
    subgraph_config.update(Config)
