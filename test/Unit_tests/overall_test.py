import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.mysql_dcerrordetector import MysqlDCErrorDetection
from pyspark.sql.types import *
from holoclean.featurization.initfeaturizer import SignalInit
from holoclean.DCFormatException import DCFormatException


holo_obj = HoloClean(
    mysql_driver="../../holoclean/lib/mysql-connector-java-5.1.44-bin.jar",
    verbose=True,
    timing_file='execution_time.txt')


class TestMysqlErrordetector(unittest.TestCase):
    def setUp(self):

        self.session = Session(holo_obj)
        self.dataset = "../../datasets/unit_test/unit_test_dataset.csv"
        self.session.load_data(self.dataset)
        self.session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_constraints.txt")

        self.detector = MysqlDCErrorDetection(self.session)
        self.session.detect_errors(self.detector)
        self.attr_constrained = \
            self.session.parser.get_all_constraint_attributes(
                 self.session.Denial_constraints)
        self.init_signal = SignalInit(self.attr_constrained,
                                      holo_obj.dataengine,
                                      self.session.dataset)

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
        dataset = "../../datasets/unit_test/unit_test_dataset.csv"
        session.load_data(dataset)
        dcs = session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_constraints.txt")
        expected = ['t1&t2&EQ(t1.A,t2.A)&IQ(t1.B,t2.B)',
                    't1&t2&EQ(t1.C,"f")&EQ(t2.C,"m")&EQ(t1.E,t2.E)']
        self.assertEqual(dcs, expected)

    def test_check_dc_format(self):
        dcs = self.session.Denial_constraints

        dc = 't1&t2&EQ(t1.A,t2.A)&IQ(t1.,t2.B)'
        try:
            self.session.parser.check_dc_format(dc, dcs)
            self.assertTrue(False)
        except DCFormatException:
            pass

        dc = 't1&t2&EQ(t1.A,t2.A)&AS(t1.B,t2.B)'
        try:
            self.session.parser.check_dc_format(dc, dcs)
            self.assertTrue(False)
        except DCFormatException:
            pass

        dc = 't1&t2&EQ(t1.A,t2.A)&IQ(t1.S,t2.B)'
        try:
            self.session.parser.check_dc_format(dc, dcs)
            self.assertTrue(False)
        except DCFormatException:
            pass

        dc = 't1&t2&EQ(t1.A,t2.A)&IQ(t1.C,t2.C)'
        self.assertEqual(dc, self.session.parser.check_dc_format(dc, dcs))

    def test_get_CNF_of_dcs(self):
        dcs = self.session.Denial_constraints
        expected = ['t1.A=t2.A AND t1.B<>t2.B',
                    't1.C="f" AND t2.C="m" AND t1.E=t2.E']
        self.assertEqual(self.session.parser.get_CNF_of_dcs(dcs), expected)

    def test_create_dc_map(self):
        dcs = self.session.Denial_constraints
        cnf_dcs = self.session.parser.get_CNF_of_dcs(dcs)
        expected = {
            't1.A=t2.A AND t1.B<>t2.B':
                [
                    [
                        't1.A=t2.A',
                        '=', 't1.A', 't2.A', 0
                    ],
                    [
                        't1.B<>t2.B',
                        '<>', 't1.B', 't2.B', 0
                    ]
                ],
            't1.C="f" AND t2.C="m" AND t1.E=t2.E':
                [
                    [
                        't1.C="f"',
                        '=', 't1.C', '"f"', 2
                    ],
                    [
                        't2.C="m"',
                        '=', 't2.C', '"m"', 2
                    ],
                    [
                        't1.E=t2.E',
                        '=', 't1.E', 't2.E', 0
                    ]
                ]
        }
        self.assertEqual(self.session.parser.create_dc_map(cnf_dcs), expected)


if __name__ == "__main__":
    unittest.main()
