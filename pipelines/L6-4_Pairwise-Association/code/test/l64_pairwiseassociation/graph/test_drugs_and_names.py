from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from l64_pairwiseassociation.graph.drugs_and_names import *
from l64_pairwiseassociation.config.ConfigStore import *


class drugs_and_namesTest(BaseTestCase):

    def test_unit_test_(self):
        dfDrug_era = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/l64_pairwiseassociation/graph/drugs_and_names/drug_era/schema.json',
            'test/resources/data/l64_pairwiseassociation/graph/drugs_and_names/drug_era/data/test_unit_test_.json',
            'drug_era'
        )
        dfC = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/l64_pairwiseassociation/graph/drugs_and_names/c/schema.json',
            'test/resources/data/l64_pairwiseassociation/graph/drugs_and_names/c/data/test_unit_test_.json',
            'c'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/l64_pairwiseassociation/graph/drugs_and_names/out/schema.json',
            'test/resources/data/l64_pairwiseassociation/graph/drugs_and_names/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = drugs_and_names(self.spark, dfDrug_era, dfC)
        assertDFEquals(
            dfOut.select("DRUG_CONCEPT_ID", "DRUG_CONCEPT_NAME", "PERSON_ID"),
            dfOutComputed.select("DRUG_CONCEPT_ID", "DRUG_CONCEPT_NAME", "PERSON_ID"),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
