from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l32_omopvocabsetup_create_vocab_map_tables.config.ConfigStore import *
from l32_omopvocabsetup_create_vocab_map_tables.udfs.UDFs import *
from prophecy.utils import *
from l32_omopvocabsetup_create_vocab_map_tables.graph import *

def pipeline(spark: SparkSession) -> None:
    df_concept_3 = concept_3(spark)
    df_concept_1 = concept_1(spark)
    df_concept_2 = concept_2(spark)
    df_concept_mapping = concept_mapping(spark, df_concept_2)
    df_concept = concept(spark)
    df_concept_relationship = concept_relationship(spark)
    df_CTE_VOCAB_MAP = CTE_VOCAB_MAP(spark, df_concept, df_concept_relationship, df_concept)
    df_source_to_concept_map = source_to_concept_map(spark)
    df_join_multiple_dataframes = join_multiple_dataframes(spark, df_source_to_concept_map, df_concept_1, df_concept_1)
    df_SetOperation_1 = SetOperation_1(spark, df_CTE_VOCAB_MAP, df_join_multiple_dataframes)
    source_to_standard_vocab_map(spark, df_SetOperation_1)
    df_source_to_concept_map_1 = source_to_concept_map_1(spark)
    df_concept_4 = concept_4(spark)
    df_join_with_multiple_inputs = join_with_multiple_inputs(
        spark, 
        df_source_to_concept_map_1, 
        df_concept_3, 
        df_concept_4
    )
    df_union_all = union_all(spark, df_concept_mapping, df_join_with_multiple_inputs)
    source_to_source_vocab_map(spark, df_union_all)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L3-2_omop-vocab-setup_create_vocab_map_tables")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/L3-2_omop-vocab-setup_create_vocab_map_tables",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
