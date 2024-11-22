from l2_omop531cdmsetup.graph.Standardized_metadata_setup.config.Config import (
    SubgraphConfig as Standardized_metadata_setup_Config
)
from l2_omop531cdmsetup.graph.Results_setup.config.Config import SubgraphConfig as Results_setup_Config
from l2_omop531cdmsetup.graph.Standardized_clinical_data_setup.config.Config import (
    SubgraphConfig as Standardized_clinical_data_setup_Config
)
from l2_omop531cdmsetup.graph.Standardized_Vocabulary_setup.config.Config import (
    SubgraphConfig as Standardized_Vocabulary_setup_Config
)
from l2_omop531cdmsetup.graph.Standardized_health_system_data_setup.config.Config import (
    SubgraphConfig as Standardized_health_system_data_setup_Config
)
from l2_omop531cdmsetup.graph.Standardized_health_economics_setup.config.Config import (
    SubgraphConfig as Standardized_health_economics_setup_Config
)
from l2_omop531cdmsetup.graph.Standardized_derived_elements_setup.config.Config import (
    SubgraphConfig as Standardized_derived_elements_setup_Config
)
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            catalog_name: str=None,
            database_name: str=None,
            Standardized_Vocabulary_setup: dict=None,
            Standardized_metadata_setup: dict=None,
            Standardized_clinical_data_setup: dict=None,
            Standardized_health_system_data_setup: dict=None,
            Standardized_health_economics_setup: dict=None,
            Standardized_derived_elements_setup: dict=None,
            Results_setup: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            catalog_name, 
            database_name, 
            Standardized_Vocabulary_setup, 
            Standardized_metadata_setup, 
            Standardized_clinical_data_setup, 
            Standardized_health_system_data_setup, 
            Standardized_health_economics_setup, 
            Standardized_derived_elements_setup, 
            Results_setup
        )

    def update(
            self,
            catalog_name: str="omop",
            database_name: str="l1_silver_omop531",
            Standardized_Vocabulary_setup: dict={},
            Standardized_metadata_setup: dict={},
            Standardized_clinical_data_setup: dict={},
            Standardized_health_system_data_setup: dict={},
            Standardized_health_economics_setup: dict={},
            Standardized_derived_elements_setup: dict={},
            Results_setup: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.Standardized_Vocabulary_setup = self.get_config_object(
            prophecy_spark, 
            Standardized_Vocabulary_setup_Config(prophecy_spark = prophecy_spark), 
            Standardized_Vocabulary_setup, 
            Standardized_Vocabulary_setup_Config
        )
        self.Standardized_metadata_setup = self.get_config_object(
            prophecy_spark, 
            Standardized_metadata_setup_Config(prophecy_spark = prophecy_spark), 
            Standardized_metadata_setup, 
            Standardized_metadata_setup_Config
        )
        self.Standardized_clinical_data_setup = self.get_config_object(
            prophecy_spark, 
            Standardized_clinical_data_setup_Config(prophecy_spark = prophecy_spark), 
            Standardized_clinical_data_setup, 
            Standardized_clinical_data_setup_Config
        )
        self.Standardized_health_system_data_setup = self.get_config_object(
            prophecy_spark, 
            Standardized_health_system_data_setup_Config(prophecy_spark = prophecy_spark), 
            Standardized_health_system_data_setup, 
            Standardized_health_system_data_setup_Config
        )
        self.Standardized_health_economics_setup = self.get_config_object(
            prophecy_spark, 
            Standardized_health_economics_setup_Config(prophecy_spark = prophecy_spark), 
            Standardized_health_economics_setup, 
            Standardized_health_economics_setup_Config
        )
        self.Standardized_derived_elements_setup = self.get_config_object(
            prophecy_spark, 
            Standardized_derived_elements_setup_Config(prophecy_spark = prophecy_spark), 
            Standardized_derived_elements_setup, 
            Standardized_derived_elements_setup_Config
        )
        self.Results_setup = self.get_config_object(
            prophecy_spark, 
            Results_setup_Config(prophecy_spark = prophecy_spark), 
            Results_setup, 
            Results_setup_Config
        )
        pass
