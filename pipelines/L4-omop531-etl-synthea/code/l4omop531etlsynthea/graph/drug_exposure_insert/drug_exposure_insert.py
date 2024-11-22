from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def drug_exposure_insert(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_source_to_standard_vocab_map_7_1 = source_to_standard_vocab_map_7_1(spark)
    df_source_to_source_vocab_map_3_1 = source_to_source_vocab_map_3_1(spark)
    df_final_visit_ids_6_1 = final_visit_ids_6_1(spark)
    df_person_7_1 = person_7_1(spark)
    df_immunizations = immunizations(spark)
    df_join_multiple_dataframes_3 = join_multiple_dataframes_3(
        spark, 
        df_immunizations, 
        df_source_to_standard_vocab_map_7_1, 
        df_source_to_source_vocab_map_3_1, 
        df_final_visit_ids_6_1, 
        df_person_7_1
    )
    df_person_6 = person_6(spark)
    df_source_to_source_vocab_map_3 = source_to_source_vocab_map_3(spark)
    df_medications = medications(spark)
    df_source_to_standard_vocab_map_7 = source_to_standard_vocab_map_7(spark)
    df_final_visit_ids_6 = final_visit_ids_6(spark)
    df_person_7 = person_7(spark)
    df_join_multiple_dataframes = join_multiple_dataframes(
        spark, 
        df_medications, 
        df_source_to_standard_vocab_map_7, 
        df_source_to_source_vocab_map_3, 
        df_final_visit_ids_6, 
        df_person_7
    )
    df_conditions_1 = conditions_1(spark)
    df_source_to_standard_vocab_map_6 = source_to_standard_vocab_map_6(spark)
    df_source_to_source_vocab_map_2 = source_to_source_vocab_map_2(spark)
    df_final_visit_ids_5 = final_visit_ids_5(spark)
    df_join_multiple_dataframes_2 = join_multiple_dataframes_2(
        spark, 
        df_conditions_1, 
        df_source_to_standard_vocab_map_6, 
        df_source_to_source_vocab_map_2, 
        df_final_visit_ids_5, 
        df_person_6
    )
    df_SetOperation_3 = SetOperation_3(
        spark, 
        df_join_multiple_dataframes_2, 
        df_join_multiple_dataframes, 
        df_join_multiple_dataframes_3
    )
    df_drug_exposures_by_person_id = drug_exposures_by_person_id(spark, df_SetOperation_3)
    drug_exposure(spark, df_drug_exposures_by_person_id)
    subgraph_config.update(Config)
