from pyspark.sql import SparkSession
from prophecy.config.utils import *
from .Config import Config as ConfigClass
Config: ConfigClass = ConfigClass()


class Utils:
    @staticmethod
    def initializeFromArgs(spark: SparkSession, args, default_conf="l2_omop531_cdm_setup.conf"):
        global Config
        Config.updateSpark(spark)
        conf = parse_config(args, default_conf)
        Config.update(**conf)
