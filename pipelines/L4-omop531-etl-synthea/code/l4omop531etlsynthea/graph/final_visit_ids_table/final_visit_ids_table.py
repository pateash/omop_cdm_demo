from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def final_visit_ids_table(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_assign_all_visit_ids_1 = assign_all_visit_ids_1(spark)
    df_visit_priority_projection = visit_priority_projection(spark, df_assign_all_visit_ids_1)
    df_encounters_with_priority = encounters_with_priority(spark, df_visit_priority_projection)
    df_first_encounter_of_visit_occurrence = first_encounter_of_visit_occurrence(spark, df_encounters_with_priority)
    df_encounter_id_and_visit_occurrence_id_projection = encounter_id_and_visit_occurrence_id_projection(
        spark, 
        df_first_encounter_of_visit_occurrence
    )
    final_visit_ids(spark, df_encounter_id_and_visit_occurrence_id_projection)
    subgraph_config.update(Config)
