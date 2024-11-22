from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, source_path: str="", target_table: str="", timestamp: int=0, **kwargs):
        self.source_path = source_path
        self.target_table = target_table
        self.timestamp = timestamp
        pass

    def update(self, updated_config):
        self.source_path = updated_config.source_path
        self.target_table = updated_config.target_table
        self.timestamp = updated_config.timestamp
        pass

Config = SubgraphConfig()
