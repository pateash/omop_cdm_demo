from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def procedure_occurrence_insert(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_person_5 = person_5(spark)
    df_procedures = procedures(spark)
    df_source_to_standard_vocab_map_5 = source_to_standard_vocab_map_5(spark)
    df_source_to_source_vocab_map_1 = source_to_source_vocab_map_1(spark)
    df_final_visit_ids_4 = final_visit_ids_4(spark)
    df_join_with_multiple_inputs = join_with_multiple_inputs(
        spark, 
        df_procedures, 
        df_source_to_standard_vocab_map_5, 
        df_source_to_source_vocab_map_1, 
        df_final_visit_ids_4, 
        df_person_5
    )
    procedure_occurence(spark, df_join_with_multiple_inputs)
    subgraph_config.update(Config)
