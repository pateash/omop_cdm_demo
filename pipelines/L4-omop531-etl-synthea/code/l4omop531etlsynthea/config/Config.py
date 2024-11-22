from l4omop531etlsynthea.graph.condition_occurrence_insert.config.Config import (
    SubgraphConfig as condition_occurrence_insert_Config
)
from l4omop531etlsynthea.graph.drug_exposure_insert.config.Config import SubgraphConfig as drug_exposure_insert_Config
from l4omop531etlsynthea.graph.all_visits_table.config.Config import SubgraphConfig as all_visits_table_Config
from l4omop531etlsynthea.graph.observation_period_insert.config.Config import (
    SubgraphConfig as observation_period_insert_Config
)
from l4omop531etlsynthea.graph.final_visit_ids_table.config.Config import SubgraphConfig as final_visit_ids_table_Config
from l4omop531etlsynthea.graph.measurement_insert.config.Config import SubgraphConfig as measurement_insert_Config
from l4omop531etlsynthea.graph.assign_all_visit_ids_table.config.Config import (
    SubgraphConfig as assign_all_visit_ids_table_Config
)
from l4omop531etlsynthea.graph.visit_occurrence_insert.config.Config import (
    SubgraphConfig as visit_occurrence_insert_Config
)
from l4omop531etlsynthea.graph.person_insert.config.Config import SubgraphConfig as person_insert_Config
from l4omop531etlsynthea.graph.procedure_occurrence_insert.config.Config import (
    SubgraphConfig as procedure_occurrence_insert_Config
)
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            catalog_name: str=None,
            database_name: str=None,
            all_visits_table: dict=None,
            assign_all_visit_ids_table: dict=None,
            final_visit_ids_table: dict=None,
            person_insert: dict=None,
            observation_period_insert: dict=None,
            visit_occurrence_insert: dict=None,
            condition_occurrence_insert: dict=None,
            measurement_insert: dict=None,
            procedure_occurrence_insert: dict=None,
            drug_exposure_insert: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            catalog_name, 
            database_name, 
            all_visits_table, 
            assign_all_visit_ids_table, 
            final_visit_ids_table, 
            person_insert, 
            observation_period_insert, 
            visit_occurrence_insert, 
            condition_occurrence_insert, 
            measurement_insert, 
            procedure_occurrence_insert, 
            drug_exposure_insert
        )

    def update(
            self,
            catalog_name: str="omop",
            database_name: str="l1_silver_omop531",
            all_visits_table: dict={},
            assign_all_visit_ids_table: dict={},
            final_visit_ids_table: dict={},
            person_insert: dict={},
            observation_period_insert: dict={},
            visit_occurrence_insert: dict={},
            condition_occurrence_insert: dict={},
            measurement_insert: dict={},
            procedure_occurrence_insert: dict={},
            drug_exposure_insert: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.all_visits_table = self.get_config_object(
            prophecy_spark, 
            all_visits_table_Config(prophecy_spark = prophecy_spark), 
            all_visits_table, 
            all_visits_table_Config
        )
        self.assign_all_visit_ids_table = self.get_config_object(
            prophecy_spark, 
            assign_all_visit_ids_table_Config(prophecy_spark = prophecy_spark), 
            assign_all_visit_ids_table, 
            assign_all_visit_ids_table_Config
        )
        self.final_visit_ids_table = self.get_config_object(
            prophecy_spark, 
            final_visit_ids_table_Config(prophecy_spark = prophecy_spark), 
            final_visit_ids_table, 
            final_visit_ids_table_Config
        )
        self.person_insert = self.get_config_object(
            prophecy_spark, 
            person_insert_Config(prophecy_spark = prophecy_spark), 
            person_insert, 
            person_insert_Config
        )
        self.observation_period_insert = self.get_config_object(
            prophecy_spark, 
            observation_period_insert_Config(prophecy_spark = prophecy_spark), 
            observation_period_insert, 
            observation_period_insert_Config
        )
        self.visit_occurrence_insert = self.get_config_object(
            prophecy_spark, 
            visit_occurrence_insert_Config(prophecy_spark = prophecy_spark), 
            visit_occurrence_insert, 
            visit_occurrence_insert_Config
        )
        self.condition_occurrence_insert = self.get_config_object(
            prophecy_spark, 
            condition_occurrence_insert_Config(prophecy_spark = prophecy_spark), 
            condition_occurrence_insert, 
            condition_occurrence_insert_Config
        )
        self.measurement_insert = self.get_config_object(
            prophecy_spark, 
            measurement_insert_Config(prophecy_spark = prophecy_spark), 
            measurement_insert, 
            measurement_insert_Config
        )
        self.procedure_occurrence_insert = self.get_config_object(
            prophecy_spark, 
            procedure_occurrence_insert_Config(prophecy_spark = prophecy_spark), 
            procedure_occurrence_insert, 
            procedure_occurrence_insert_Config
        )
        self.drug_exposure_insert = self.get_config_object(
            prophecy_spark, 
            drug_exposure_insert_Config(prophecy_spark = prophecy_spark), 
            drug_exposure_insert, 
            drug_exposure_insert_Config
        )
        pass
