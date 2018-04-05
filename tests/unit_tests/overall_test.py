import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from pyspark.sql.types import *
from holoclean.featurization.initfeaturizer import SignalInit
from holoclean.DCFormatException import DCFormatException
from holoclean.utils.parser_interface import DenialConstraint


holo_obj = HoloClean(
    holoclean_path="../..",
    verbose=True,
    timing_file='execution_time.txt')


class TestMysqlErrordetector(unittest.TestCase):
    def setUp(self):

        self.session = Session(holo_obj)
        self.dataset = "../data/unit_test/unit_test_dataset.csv"
        self.session.load_data(self.dataset)
        self.session.load_denial_constraints(
            "../data/unit_test/unit_test_constraints.txt")
        self.detector_list = []
        self.detector_list.append(SqlDCErrorDetection(self.session))
        self.session.detect_errors(self.detector_list)

        self.init_signal = SignalInit(self.session)

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

    def test_load_denial_constraints(self):
        session = Session(holo_obj)
        dataset = "../data/unit_test/unit_test_dataset.csv"
        session.load_data(dataset)
        dcs = session.load_denial_constraints(
            "../data/unit_test/unit_test_constraints.txt")
        expected = ["t1&t2&EQ(t1.A,t2.A)&IQ(t1.B,t2.B)",
                    "t1&t2&EQ(t1.C,'f')&EQ(t2.C,'m')&EQ(t1.E,t2.E)"]
        self.assertEqual(dcs, expected)

    def test_check_dc_format(self):

        dc = 't1&t2&EQ(t1.A,t2.A)&IQ(t1.,t2.B)'
        try:
            DenialConstraint(dc, self.session.dataset.attributes['Init'])
            self.assertTrue(False)
        except DCFormatException:
            pass

        dc = 't1&t2&EQ(t1.A,t2.A)&AS(t1.B,t2.B)'
        try:
            DenialConstraint(dc, self.session.dataset.attributes['Init'])
            self.assertTrue(False)
        except DCFormatException:
            pass

        dc = 't1&t2&EQ(t1.A,t2.A)&IQ(t1.S,t2.B)'
        try:
            DenialConstraint(dc, self.session.dataset.attributes['Init'])
            self.assertTrue(False)
        except DCFormatException:
            pass

        dc = 't1&t2&EQ(t1.A,t2.A)&IQ(t1.C,t2.C)'
        DenialConstraint(dc, self.session.dataset.attributes['Init'])


if __name__ == "__main__":
    unittest.main()
