import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from holoclean.featurization.initfeaturizer import SignalInit
from pyspark.sql.types import *

holo_obj = HoloClean(
    holoclean_path="../..",
    verbose=True,
    timing_file='execution_time.txt')


class TestInitFeaturizer(unittest.TestCase):
    def setUp(self):

        self.session = Session(holo_obj)
        self.dataset = "../data/unit_test/unit_test_dataset.csv"
        self.session.load_data(self.dataset)
        self.session.load_denial_constraints(
            "../data/unit_test/unit_test_constraints.txt")

        detector = SqlDCErrorDetection(self.session)
        self.session.detect_errors([detector])
        self.session._ds_domain_pruning(holo_obj.pruning_threshold1,
                                        holo_obj.pruning_threshold2,
                                        holo_obj.pruning_dk_breakoff,
                                        holo_obj.pruning_clean_breakoff)

        self.init_signal = SignalInit(self.session)


    def tearDown(self):
        del self.session

    def test_Init_query_for_clean(self):
        query = self.init_signal.get_query()[0]

        Init_feature_dataframe = \
            holo_obj.dataengine.query(query, 1)

        anticipated_Init_feature_C_clean_cells = [
            ["1", "2", "1", "1"], ["2", "1", "1", "1"]]
        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_Init_feature_C_clean_cells, StructType([
                StructField("vid", StringType(), False),
                StructField("assigned_val", StringType(), False),
                StructField("feature", StringType(), False),
                StructField("count", StringType(), False),
            ]))
        incorrect = anticipated_dataframe.subtract(
            Init_feature_dataframe)
        self.assertEquals(incorrect.count(), 0)

    def test_Init_query_for_dk(self):
        query = self.init_signal.get_query(0)[0]
        Init_feature_dataframe = \
            holo_obj.dataengine.query(query, 1)

        anticipated_Init_feature_C_dk_cells = [["1", "1", "1", "1"],
                                                ["2", "3", "1", "1"],
                                                ["3", "2", "1", "1"],
                                                ["4", "1", "1", "1"],
                                                ["5", "1", "1", "1"],
                                                ["6", "2", "1", "1"],
                                                ["7", "2", "1", "1"],
                                                ["8", "1", "1", "1"],
                                                ["9", "1", "1", "1"],
                                                ["10", "1", "1", "1"]]
        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_Init_feature_C_dk_cells, StructType([
                StructField("vid", StringType(), False),
                StructField("assigned_val", StringType(), False),
                StructField("feature", StringType(), False),
                StructField("count", StringType(), False),
            ]))
        incorrect = anticipated_dataframe.subtract(
            Init_feature_dataframe)
        self.assertEquals(incorrect.count(), 0)


if __name__ == "__main__":
    unittest.main()
