from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l42_omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def cteDrugExposureEnds(
        spark: SparkSession,
        subgraph_config: SubgraphConfig,
        dt: DataFrame,
        e0: DataFrame
) -> DataFrame:
    Config.update(subgraph_config)
    df_by_person_and_ingredient_concept_id = by_person_and_ingredient_concept_id(spark, dt, e0)
    df_sub_exposure_end_by_person_and_ingredient = sub_exposure_end_by_person_and_ingredient(
        spark, 
        df_by_person_and_ingredient_concept_id
    )
    df_drug_exposure_details = drug_exposure_details(spark, df_sub_exposure_end_by_person_and_ingredient)
    subgraph_config.update(Config)

    return df_drug_exposure_details
