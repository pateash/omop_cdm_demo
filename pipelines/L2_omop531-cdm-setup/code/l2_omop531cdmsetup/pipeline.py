from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l2_omop531cdmsetup.config.ConfigStore import *
from l2_omop531cdmsetup.udfs.UDFs import *
from prophecy.utils import *
from l2_omop531cdmsetup.graph import *

def pipeline(spark: SparkSession) -> None:
    Standardized_metadata_setup(spark, Config.Standardized_metadata_setup)
    Results_setup(spark, Config.Results_setup)
    Standardized_clinical_data_setup(spark, Config.Standardized_clinical_data_setup)
    Standardized_Vocabulary_setup(spark, Config.Standardized_Vocabulary_setup)
    Standardized_health_system_data_setup(spark, Config.Standardized_health_system_data_setup)
    df_metadata_version_creation = metadata_version_creation(spark)
    metadata_update(spark, df_metadata_version_creation)
    Standardized_health_economics_setup(spark, Config.Standardized_health_economics_setup)
    Standardized_derived_elements_setup(spark, Config.Standardized_derived_elements_setup)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L2_omop531-cdm-setup")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/L2_omop531-cdm-setup", config = Config)(pipeline)

if __name__ == "__main__":
    main()
