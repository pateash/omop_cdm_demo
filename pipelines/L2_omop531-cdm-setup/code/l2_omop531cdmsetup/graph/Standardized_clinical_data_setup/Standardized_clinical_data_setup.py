from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l2_omop531cdmsetup.udfs.UDFs import *
from . import *
from .config import *

def Standardized_clinical_data_setup(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_specimen_schema = specimen_schema(spark)
    specimen(spark, df_specimen_schema)
    df_device_exposure_schema = device_exposure_schema(spark)
    df_drug_exposure_schema = drug_exposure_schema(spark)
    df_death_schema = death_schema(spark)
    df_measurement_schema = measurement_schema(spark)
    df_visit_detail_schema = visit_detail_schema(spark)
    visit_detail(spark, df_visit_detail_schema)
    device_exposure(spark, df_device_exposure_schema)
    df_note_nlp_schema = note_nlp_schema(spark)
    df_person_schema = person_schema(spark)
    df_procedure_occurence_schema = procedure_occurence_schema(spark)
    procedure_occurence(spark, df_procedure_occurence_schema)
    note_nlp(spark, df_note_nlp_schema)
    df_observation_period_schema = observation_period_schema(spark)
    observation_period(spark, df_observation_period_schema)
    person(spark, df_person_schema)
    death(spark, df_death_schema)
    drug_exposure(spark, df_drug_exposure_schema)
    df_visit_occurence_schema = visit_occurence_schema(spark)
    visit_occurence(spark, df_visit_occurence_schema)
    df_condition_occurence_schema = condition_occurence_schema(spark)
    condition_occurence(spark, df_condition_occurence_schema)
    df_fact_relationship_schema = fact_relationship_schema(spark)
    df_note_schema = note_schema(spark)
    note(spark, df_note_schema)
    df_observation_schema = observation_schema(spark)
    measurement(spark, df_measurement_schema)
    fact_relationship(spark, df_fact_relationship_schema)
    Optimize_Observation_Table(spark)
    observation(spark, df_observation_schema)
    subgraph_config.update(Config)
