from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            catalog_name: str="omop",
            database_name: str="l1_silver_omop531results",
            **kwargs
    ):
        self.catalog_name = catalog_name
        self.database_name = database_name
        pass

    def update(self, updated_config):
        self.catalog_name = updated_config.catalog_name
        self.database_name = updated_config.database_name
        pass

Config = SubgraphConfig()
