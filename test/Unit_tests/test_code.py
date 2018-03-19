import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.mysql_dcerrordetector import MysqlDCErrorDetection
from pyspark.sql.types import *
from holoclean.DCFormatException import DCFormatException
from holoclean.featurization.initfeaturizer import SignalInit
from holoclean.featurization.dcfeaturizer import SignalDC



holo_obj = HoloClean(
    mysql_driver="../../holoclean/lib/mysql-connector-java-5.1.44-bin.jar",
    verbose=True,
    timing_file='execution_time.txt')


class TestDCFeaturizer(unittest.TestCase):
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

    def test_DC_query_for_clean(self):

        dc_signal = SignalDC(self.session.Denial_constraints, self.session)

        self.session._ds_domain_pruning(0.5)
        self.session._add_featurizer(dc_signal)

        temp_list = dc_signal._create_all_relaxed_dc()
        relaxed_dcs = []
        for relaxed_dc in temp_list:
            relaxed_dcs.append(relaxed_dc[0])

        expected_r_dcs = \
            ["postab.tid = t1.index AND postab.attr_name ='A' AND"
             " postab.attr_val=t2.A AND  t1.B<>t2.B",
             "postab.tid = t2.index AND postab.attr_name = 'A' AND"
             " t1.A=postab.attr_val AND  t1.B<>t2.B",
             "postab.tid = t1.index AND postab.attr_name ='B' AND"
             " postab.attr_val<>t2.B AND  t1.A=t2.A",
             "postab.tid = t2.index AND postab.attr_name = 'B' AND"
             " t1.B<>postab.attr_val AND  t1.A=t2.A",
             'postab.tid = t1.index AND postab.attr_name =\'C\' AND'
             ' postab.attr_val="f" AND  t2.C="m" AND  t1.E=t2.E',
             'postab.tid = t2.index AND postab.attr_name =\'C\' AND'
             ' postab.attr_val="m" AND  t1.C="f" AND  t1.E=t2.E',
             'postab.tid = t1.index AND postab.attr_name =\'E\' AND'
             ' postab.attr_val=t2.E AND  t1.C="f" AND  t2.C="m"',
             'postab.tid = t2.index AND postab.attr_name = \'E\' AND'
             ' t1.E=postab.attr_val AND  t1.C="f" AND  t2.C="m"'
             ]

        self.assertEquals(relaxed_dcs, expected_r_dcs)


class TestDCFeaturizerNonSymmetric(unittest.TestCase):
    def setUp(self):

        self.session = Session(holo_obj)
        self.dataset = "../../datasets/unit_test/unit_test_dataset.csv"
        self.session.load_data(self.dataset)
        self.session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_non_symmetric_constraints.txt")

        detector = MysqlDCErrorDetection(self.session.Denial_constraints,
                                         holo_obj,
                                         self.session.dataset)
        self.session.detect_errors(detector)

    def test_DC_query_for_clean(self):
        attr_constrained = self.session.parser.get_all_constraint_attributes(
            self.session.Denial_constraints)

        dc_signal = SignalDC(self.session.Denial_constraints, self.session)

        self.session._ds_domain_pruning(0.5)
        self.session._add_featurizer(dc_signal)

        temp_list = dc_signal._create_all_relaxed_dc()
        relaxed_dcs = []
        for relaxed_dc in temp_list:
            relaxed_dcs.append(relaxed_dc[0])

        expected_r_dcs = \
            ["postab.tid = t1.index AND postab.attr_name ='A' AND"
             " postab.attr_val=t2.A AND  t1.B>t2.B",
             "postab.tid = t2.index AND postab.attr_name = 'A' AND"
             " t1.A=postab.attr_val AND  t1.B>t2.B",
             "postab.tid = t1.index AND postab.attr_name ='B' AND"
             " postab.attr_val>t2.B AND  t1.A=t2.A",
             "postab.tid = t2.index AND postab.attr_name = 'B' AND"
             " t1.B>postab.attr_val AND  t1.A=t2.A",
             'postab.tid = t1.index AND postab.attr_name =\'C\' AND'
             ' postab.attr_val>="f" AND  t2.C<="m" AND  t1.E=t2.E',
             'postab.tid = t2.index AND postab.attr_name =\'C\' AND'
             ' postab.attr_val<="m" AND  t1.C>="f" AND  t1.E=t2.E',
             'postab.tid = t1.index AND postab.attr_name =\'E\' AND'
             ' postab.attr_val=t2.E AND  t1.C>="f" AND  t2.C<="m"',
             'postab.tid = t2.index AND postab.attr_name = \'E\' AND'
             ' t1.E=postab.attr_val AND  t1.C>="f" AND  t2.C<="m"'
             ]

        self.assertEquals(relaxed_dcs, expected_r_dcs)


class TestMysqlErrordetector(unittest.TestCase):
    def setUp(self):

        self.session = Session(holo_obj)
        self.dataset = "../../datasets/unit_test/unit_test_dataset.csv"
        self.session.load_data(self.dataset)
        self.session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_constraints.txt")

        self.detector = MysqlDCErrorDetection(self.session.Denial_constraints,
                                              holo_obj,
                                              self.session.dataset)
        self.session.detect_errors(self.detector)

        self.attr_constrained = \
            self.session.parser.get_all_constraint_attributes(
                 self.session.Denial_constraints)
        self.init_signal = SignalInit(self.attr_constrained,
                                      holo_obj.dataengine,
                                      self.session.dataset)

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

    def test_Init_query_for_clean(self):
        query = self.init_signal.get_query()
        self.session._ds_domain_pruning(0.5)
        Int_feature_dataframe = \
            holo_obj.dataengine.query(query[2:], 1)

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
            Int_feature_dataframe)
        self.assertEquals(incorrect.count(), 0)

    def test_Init_query_for_dk(self):
        query = self.init_signal.get_query(0)
        self.session._ds_domain_pruning(0.5)
        Int_feature_dataframe = \
            holo_obj.dataengine.query(query[2:], 1)

        anticipated_Init_feature_C_dk_cells = [["1", "1", "1", "1"],
                                               ["2", "1", "1", "1"],
                                               ["3", "1", "1", "1"],
                                               ["4", "1", "1", "1"],
                                               ["5", "1", "1", "1"],
                                               ["6", "1", "1", "1"],
                                               ["7", "1", "1", "1"],
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
            Int_feature_dataframe)
        self.assertEquals(incorrect.count(), 0)


if __name__ == "__main__":
    unittest.main()
