from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l1_cdm_setup.config.ConfigStore import *
from l1_cdm_setup.udfs.UDFs import *
from prophecy.utils import *
from l1_cdm_setup.graph import *

def pipeline(spark: SparkSession) -> None:
    df_read_omop_vocabs_from_s3_1 = read_omop_vocabs_from_s3_1(spark)
    df_extract_table_name_from_path_1 = extract_table_name_from_path_1(spark, df_read_omop_vocabs_from_s3_1)
    TableIterator_1_1(Config.TableIterator_1_1).apply(spark, df_extract_table_name_from_path_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L1_cdm_setup")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/L1_cdm_setup", config = Config)(pipeline)

if __name__ == "__main__":
    main()
