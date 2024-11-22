from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l42_omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def cteSubExposures(spark: SparkSession, subgraph_config: SubgraphConfig, cteDrugExposureEnds: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_drug_exposure_summary = drug_exposure_summary(spark, cteDrugExposureEnds)
    df_drug_exposure_summary_reformatted = drug_exposure_summary_reformatted(spark, df_drug_exposure_summary)
    subgraph_config.update(Config)

    return df_drug_exposure_summary_reformatted
