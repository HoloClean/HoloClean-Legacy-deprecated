import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.mysql_dcerrordetector import MysqlDCErrorDetection
from holoclean.featurization.cooccurrencefeaturizer import SignalCooccur
from pyspark.sql.types import *

holo_obj = HoloClean(
    mysql_driver="../../holoclean/lib/mysql-connector-java-5.1.44-bin.jar",
    verbose=True,
    timing_file='execution_time.txt')


class TestCooccurFeaturizer(unittest.TestCase):
    def setUp(self):

        self.session = Session(holo_obj)
        self.dataset = "../../datasets/unit_test/unit_test_dataset.csv"
        self.session.load_data(self.dataset)
        self.session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_constraints.txt")

        detector = MysqlDCErrorDetection(self.session.Denial_constraints,
                                         holo_obj,
                                         self.session.dataset)
        self.session.detect_errors(detector)
        self.attr_constrained =\
            self.session.parser.get_all_constraint_attributes(
                 self.session.Denial_constraints)

    def tearDown(self):
        del self.session

    def test_Cooccur_query_for_clean(self):
        cooccur_signal = SignalCooccur(self.attr_constrained,
                                       holo_obj.dataengine,
                                       self.session.dataset)

        query = cooccur_signal.get_query()
        self.session._ds_domain_pruning(0.5)
        self.session._add_featurizer(cooccur_signal)

        Cooccur_feature_dataframe = \
            holo_obj.dataengine.query(query[2:], 1)

        anticipated_C_clean_cells = [["1", "2", "7", "1", ],
                                     ["1", "2", "9", "1"],
                                     ["2", "1", "2", "1"],
                                     ["2", "1", "9", "1"]]
        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_C_clean_cells, StructType([
                StructField("vid", StringType(), False),
                StructField("assigned_val", StringType(), False),
                StructField("feature", StringType(), False),
                StructField("count", StringType(), False),
            ]))
        incorrect = anticipated_dataframe.subtract(Cooccur_feature_dataframe)
        self.assertEquals(incorrect.count(), 0)

    def test_Cooccur_query_for_dk(self):

        cooccur_signal = SignalCooccur(self.attr_constrained,
                                       holo_obj.dataengine,
                                       self.session.dataset)

        query = cooccur_signal.get_query(0)
        self.session._ds_domain_pruning(0.5)
        self.session._add_featurizer(cooccur_signal)
        Cooccur_feature_dataframe = \
            holo_obj.dataengine.query(query[2:], 1)

        anticipated_C_dk_cells = [[1, 1, 3, 1], [1, 1, 5, 1],
                                  [1, 1, 8, 1], [2, 1, 1, 1],
                                  [2, 1, 3, 1], [2, 1, 8, 1],
                                  [3, 1, 1, 1], [3, 1, 5, 1],
                                  [3, 1, 8, 1], [4, 1, 1, 1],
                                  [4, 1, 3, 1], [4, 1, 5, 1],
                                  [5, 1, 3, 1], [5, 1, 6, 1],
                                  [5, 1, 8, 1], [6, 1, 1, 1],
                                  [6, 1, 3, 1], [6, 1, 8, 1],
                                  [7, 1, 1, 1], [7, 1, 6, 1],
                                  [7, 1, 8, 1], [8, 1, 1, 1],
                                  [8, 1, 3, 1], [8, 1, 6, 1],
                                  [9, 1, 8, 1], [10, 1, 4, 1]]
        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_C_dk_cells, StructType([
                StructField("vid", StringType(), False),
                StructField("assigned_val", StringType(), False),
                StructField("feature", StringType(), False),
                StructField("count", StringType(), False),
            ]))
        incorrect = anticipated_dataframe.subtract(Cooccur_feature_dataframe)
        self.assertEquals(incorrect.count(), 0)


if __name__ == "__main__":
    unittest.main()
