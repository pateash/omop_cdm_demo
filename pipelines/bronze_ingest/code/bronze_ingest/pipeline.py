from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from bronze_ingest.config.ConfigStore import *
from bronze_ingest.udfs.UDFs import *
from prophecy.utils import *
from bronze_ingest.graph import *

def pipeline(spark: SparkSession) -> None:
    df_read_all_states_90k = read_all_states_90k(spark)
    df_source_path_projection = source_path_projection(spark, df_read_all_states_90k)
    TableIterator_1(Config.TableIterator_1).apply(spark, df_source_path_projection)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/bronze_ingest")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/bronze_ingest", config = Config)(pipeline)

if __name__ == "__main__":
    main()
