from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def tmp(spark: SparkSession, subgraph_config: SubgraphConfig) -> DataFrame:
    Config.update(subgraph_config)
    df_source_to_standard_vocab_map_2 = source_to_standard_vocab_map_2(spark)
    df_source_to_standard_vocab_map_1_1 = source_to_standard_vocab_map_1_1(spark)
    df_procedures_1 = procedures_1(spark)
    df_source_to_source_vocab_map_1_1 = source_to_source_vocab_map_1_1(spark)
    df_final_visit_ids_3_1 = final_visit_ids_3_1(spark)
    df_person_4_1 = person_4_1(spark)
    df_join_multiple_dataframes_1 = join_multiple_dataframes_1(
        spark, 
        df_procedures_1, 
        df_source_to_standard_vocab_map_1_1, 
        df_source_to_source_vocab_map_1_1, 
        df_final_visit_ids_3_1, 
        df_person_4_1
    )
    df_observations = observations(spark)
    df_source_to_standard_vocab_map_1 = source_to_standard_vocab_map_1(spark)
    df_source_to_standard_vocab_map_3 = source_to_standard_vocab_map_3(spark)
    df_source_to_standard_vocab_map_4 = source_to_standard_vocab_map_4(spark)
    df_final_visit_ids_3 = final_visit_ids_3(spark)
    df_person_4 = person_4(spark)
    df_join_multiple_dataframes = join_multiple_dataframes(
        spark, 
        df_observations, 
        df_source_to_standard_vocab_map_1, 
        df_source_to_standard_vocab_map_2, 
        df_source_to_standard_vocab_map_3, 
        df_source_to_standard_vocab_map_4, 
        df_final_visit_ids_3, 
        df_person_4
    )
    df_SetOperation_2 = SetOperation_2(spark, df_join_multiple_dataframes_1, df_join_multiple_dataframes)
    subgraph_config.update(Config)

    return df_SetOperation_2
