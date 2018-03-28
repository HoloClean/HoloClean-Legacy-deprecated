import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from pyspark.sql.types import *

holo_obj = HoloClean(
            holoclean_path="../..",
            verbose=True,
            timing_file='execution_time.txt',
            learning_iterations=50,
            learning_rate=0.001,
            batch_size=20)

class TestMysqlErrordetector(unittest.TestCase):
    def setUp(self):

        self.session = Session(holo_obj)
        self.dataset = "../../datasets/unit_test/unit_test_dataset.csv"
        self.session.load_data(self.dataset)
        self.session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_constraints.txt")

        self.detector = SqlDCErrorDetection(self.session)
        self.session.detect_errors([self.detector])

    def tearDown(self):
        del self.session

    def test_number_of_dk_cells(self):
        dataframe_C_dk = holo_obj.dataengine.get_table_to_dataframe(
            'C_dk', self.session.dataset)
        self.assertEquals(dataframe_C_dk.count(), 10)

    def test_number_of_clean_cells(self):
        dataframe_C_clean = holo_obj.dataengine.get_table_to_dataframe(
            'C_clean', self.session.dataset)
        self.assertEquals(dataframe_C_clean.count(), 5)

    def test_correction_of_clean_cells(self):
        dataframe_C_clean = holo_obj.dataengine.get_table_to_dataframe(
            'C_clean', self.session.dataset)

        anticipated_C_clean_cells = [["3", "D"], ["1", "D"], ["2", "D"],
                                     ["3", "A"], ["3", "B"]]

        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_C_clean_cells, StructType([
                StructField("ind", StringType(), False),
                StructField("attr", StringType(), False),
            ]))

        incorrect = anticipated_dataframe.subtract(dataframe_C_clean)
        self.assertEquals(incorrect.count(), 0)

    def test_correction_of_dk_cells(self):
        dataframe_C_dk = holo_obj.dataengine.get_table_to_dataframe(
            'C_dk', self.session.dataset)

        anticipated_dataframe_C_dk_cells = [["3", "C"],
                                            ["2", "C"], ["2", "A"], ["2", "E"],
                                            ["3", "E"], ["2", "B"], ["1", "A"],
                                            ["1", "C"], ["1", "B"], ["1", "E"]]

        anticipated_dataframe = holo_obj.spark_session.createDataFrame(
            anticipated_dataframe_C_dk_cells, StructType([
                StructField("ind", StringType(), False),
                StructField("attr", StringType(), False),
            ]))

        incorrect = anticipated_dataframe.subtract(dataframe_C_dk)
        self.assertEquals(incorrect.count(), 0)


if __name__ == "__main__":
    unittest.main()
