from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, catalog_name: str=None, database_name: str=None, **kwargs):
        self.spark = None
        self.update(catalog_name, database_name)

    def update(self, catalog_name: str="omop", database_name: str="l1_silver_omop531", **kwargs):
        prophecy_spark = self.spark
        self.catalog_name = catalog_name
        self.database_name = database_name
        pass
