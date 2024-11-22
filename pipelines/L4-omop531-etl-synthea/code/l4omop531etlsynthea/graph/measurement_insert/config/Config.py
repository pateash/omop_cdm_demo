from l4omop531etlsynthea.graph.measurement_insert.tmp.config.Config import SubgraphConfig as tmp_Config
from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            catalog_name: str="omop",
            database_name: str="l1_silver_omop531",
            tmp: dict={},
            **kwargs
    ):
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.tmp = self.get_config_object(prophecy_spark, tmp_Config(prophecy_spark = prophecy_spark), tmp, tmp_Config)
        pass

    def update(self, updated_config):
        self.catalog_name = updated_config.catalog_name
        self.database_name = updated_config.database_name
        self.tmp = updated_config.tmp
        pass

Config = SubgraphConfig()
