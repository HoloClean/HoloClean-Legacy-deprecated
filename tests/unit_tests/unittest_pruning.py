import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from pyspark.sql.types import *


holo_obj = HoloClean(
    holoclean_path="../..",
    verbose=True,
    timing_file='execution_time.txt')


class TestPruning(unittest.TestCase):
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

    def test_possible_values_clean(self):
        possible_values_clean = holo_obj.dataengine.get_table_to_dataframe(
            "Possible_values_clean", self.session.dataset)
        anticipated_possible_values_clean = [["1", "3", "A", "p", "0", "1"],
                                             ["1", "3", "A", "u", "1", "2"],
                                             ["2", "3", "B", "y", "1", "1"],
                                             ["2", "3", "B", "z", "0", "2"],
                                             ["2", "3", "B", "w", "0", "3"]]

        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_possible_values_clean, StructType([
                StructField("vid", StringType(), False),
                StructField("tid", StringType(), False),
                StructField("attr_name", StringType(), False),
                StructField("attr_val", StringType(), False),
                StructField("observed", StringType(), False),
                StructField("domain_id", StringType(), False),
            ]))
        incorrect = anticipated_dataframe.subtract(
            possible_values_clean)
        self.assertEquals(incorrect.count(), 0)

    def test_possible_values_dk(self):
        possible_values_dk = holo_obj.dataengine.get_table_to_dataframe(
            "Possible_values_dk", self.session.dataset)
        anticipated_possible_values_dk = [["1", "1", "A", "p", "1", "1"],
                                          ["1", "1", "A", "u", "0", "2"],
                                          ["2", "1", "B", "y", "0", "1"],
                                          ["2", "1", "B", "z", "0", "2"],
                                          ["2", "1", "B", "w", "1", "3"],
                                          ["3", "1", "C", "m", "0", "1"],
                                          ["3", "1", "C", "f", "1", "2"],
                                          ["4", "1", "E", "r", "1", "1"],
                                          ["5", "2", "A", "p", "1", "1"],
                                          ["5", "2", "A", "u", "0", "2"],
                                          ["6", "2", "B", "y", "0", "1"],
                                          ["6", "2", "B", "z", "1", "2"],
                                          ["6", "2", "B", "w", "0", "3"],
                                          ["7", "2", "C", "m", "0", "1"],
                                          ["7", "2", "C", "f", "1", "2"],
                                          ["8", "2", "E", "r", "1", "1"],
                                          ["9", "3", "C", "m", "1", "1"],
                                          ["9", "3", "C", "f", "0", "2"],
                                          ["10", "3", "E", "r", "1", "1"]]
        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_possible_values_dk, StructType([
                StructField("vid", StringType(), False),
                StructField("tid", StringType(), False),
                StructField("attr_name", StringType(), False),
                StructField("attr_val", StringType(), False),
                StructField("observed", StringType(), False),
                StructField("domain_id", StringType(), False),
            ]))
        incorrect = anticipated_dataframe.subtract(
            possible_values_dk)
        self.assertEquals(incorrect.count(), 0)

    def test_kij_dk(self):
        kij_dk = holo_obj.dataengine.get_table_to_dataframe(
            "Kij_lookup_dk", self.session.dataset)
        anticipated_kij_dk = [["1", "1",  "A", "2"],
                              ["2", "1", "B", "3"],
                              ["3", "1", "C", "2"],
                              ["4", "1", "E", "1"],
                              ["5", "2", "A", "2"],
                              ["6", "2", "B", "3"],
                              ["7", "2", "C", "2"],
                              ["8", "2", "E", "1"],
                              ["9", "3", "C", "2"],
                              ["10", "3", "E", "1"]]

        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_kij_dk, StructType([
                StructField("vid", StringType(), False),
                StructField("tid", StringType(), False),
                StructField("attr_name", StringType(), False),
                StructField("k_ij", StringType(), False),
            ]))
        incorrect = anticipated_dataframe.subtract(
            kij_dk)
        self.assertEquals(incorrect.count(), 0)

    def test_kij_clean(self):
        kij_clean = holo_obj.dataengine.get_table_to_dataframe(
            "Kij_lookup_clean", self.session.dataset)
        anticipated_kij_clean = [["1", "3",  "A", "2"],
                                 ["2", "3", "B", "3"]]
        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_kij_clean, StructType([
                StructField("vid", StringType(), False),
                StructField("tid", StringType(), False),
                StructField("attr_name", StringType(), False),
                StructField("k_ij", StringType(), False),
            ]))
        incorrect = anticipated_dataframe.subtract(
            kij_clean)
        self.assertEquals(incorrect.count(), 0)


if __name__ == "__main__":
    unittest.main()
