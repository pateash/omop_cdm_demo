from l4omop531etlsynthea.graph.all_visits_table.OP_VISITS.config.Config import SubgraphConfig as OP_VISITS_Config
from l4omop531etlsynthea.graph.all_visits_table.IP_VISITS.config.Config import SubgraphConfig as IP_VISITS_Config
from l4omop531etlsynthea.graph.all_visits_table.ER_VISITS.config.Config import SubgraphConfig as ER_VISITS_Config
from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            IP_VISITS: dict={},
            ER_VISITS: dict={},
            OP_VISITS: dict={},
            catalog_name: str="omop",
            database_name: str="l1_silver_omop531",
            **kwargs
    ):
        self.IP_VISITS = self.get_config_object(
            prophecy_spark, 
            IP_VISITS_Config(prophecy_spark = prophecy_spark), 
            IP_VISITS, 
            IP_VISITS_Config
        )
        self.ER_VISITS = self.get_config_object(
            prophecy_spark, 
            ER_VISITS_Config(prophecy_spark = prophecy_spark), 
            ER_VISITS, 
            ER_VISITS_Config
        )
        self.OP_VISITS = self.get_config_object(
            prophecy_spark, 
            OP_VISITS_Config(prophecy_spark = prophecy_spark), 
            OP_VISITS, 
            OP_VISITS_Config
        )
        self.catalog_name = catalog_name
        self.database_name = database_name
        pass

    def update(self, updated_config):
        self.IP_VISITS = updated_config.IP_VISITS
        self.ER_VISITS = updated_config.ER_VISITS
        self.OP_VISITS = updated_config.OP_VISITS
        self.catalog_name = updated_config.catalog_name
        self.database_name = updated_config.database_name
        pass

Config = SubgraphConfig()
