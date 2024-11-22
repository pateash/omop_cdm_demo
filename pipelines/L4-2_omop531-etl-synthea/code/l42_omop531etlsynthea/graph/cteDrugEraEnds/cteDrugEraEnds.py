from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l42_omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def cteDrugEraEnds(spark: SparkSession, subgraph_config: SubgraphConfig, ft: DataFrame, e0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_by_person_id_and_drug_concept_id = by_person_id_and_drug_concept_id(spark, ft, e0)
    df_era_end_datetime_by_person_and_drug = era_end_datetime_by_person_and_drug(
        spark, 
        df_by_person_id_and_drug_concept_id
    )
    df_drug_era_summary = drug_era_summary(spark, df_era_end_datetime_by_person_and_drug)
    subgraph_config.update(Config)

    return df_drug_era_summary
