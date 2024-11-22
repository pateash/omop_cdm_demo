from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def condition_occurrence_insert(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_final_visit_ids_2 = final_visit_ids_2(spark)
    df_person_3 = person_3(spark)
    df_conditions = conditions(spark)
    df_source_to_standard_vocab_map = source_to_standard_vocab_map(spark)
    df_source_to_source_vocab_map = source_to_source_vocab_map(spark)
    df_join_with_multiple_inputs = join_with_multiple_inputs(
        spark, 
        df_conditions, 
        df_source_to_standard_vocab_map, 
        df_source_to_source_vocab_map, 
        df_final_visit_ids_2, 
        df_person_3
    )
    condition_occurence(spark, df_join_with_multiple_inputs)
    subgraph_config.update(Config)
