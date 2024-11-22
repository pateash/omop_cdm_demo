from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l64_pairwiseassociation.config.ConfigStore import *
from l64_pairwiseassociation.udfs.UDFs import *
from prophecy.utils import *
from l64_pairwiseassociation.graph import *

def pipeline(spark: SparkSession) -> None:
    df_drug_era = drug_era(spark)
    df_concept = concept(spark)
    df_drugs_and_names = drugs_and_names(spark, df_drug_era, df_concept)
    df_drugs = drugs(spark, df_drugs_and_names, df_drugs_and_names)
    df_counts = counts(spark, df_drugs)
    df_by_count_d1d2_asc = by_count_d1d2_asc(spark, df_counts)
    df_pairwise_sums = pairwise_sums(spark, df_by_count_d1d2_asc)
    df_COPRESCRIBED = COPRESCRIBED(spark, df_pairwise_sums)
    COPRESCRIBED_1(spark, df_COPRESCRIBED)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("L6-4_Pairwise-Association")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L6-4_Pairwise-Association")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/L6-4_Pairwise-Association", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
