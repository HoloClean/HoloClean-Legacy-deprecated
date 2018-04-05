import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from holoclean.featurization.dcfeaturizer import SignalDC
from holoclean.global_variables import GlobalVariables

holo_obj = HoloClean(
    holoclean_path="../..",
    verbose=True,
    timing_file='execution_time.txt')


class TestDCFeaturizer(unittest.TestCase):
    def setUp(self):

        self.session = Session(holo_obj)
        self.dataset = "../data/unit_test/unit_test_dataset.csv"
        self.session.load_data(self.dataset)
        self.session.load_denial_constraints(
            "../data/unit_test/unit_test_non_symmetric_constraints.txt")

        detector = SqlDCErrorDetection(self.session)
        self.session.detect_errors([detector])
        self.session._ds_domain_pruning(holo_obj.pruning_threshold1,
                                        holo_obj.pruning_threshold2,
                                        holo_obj.pruning_dk_breakoff,
                                        holo_obj.pruning_clean_breakoff)

    def test_DC_query_for_clean(self):

        dc_signal = SignalDC(self.session.Denial_constraints, self.session)

        self.session._add_featurizer(dc_signal)

        temp_list = dc_signal._create_all_relaxed_dc()
        relaxed_dcs = []
        for relaxed_dc in temp_list:
            relaxed_dcs.append(relaxed_dc[0])

        expected_r_dcs = \
            ["postab.tid = t1." + GlobalVariables.index_name +
             " AND postab.attr_name = 'A' AND postab.attr_val=t2.A AND  t1." +
             GlobalVariables.index_name + " < t2." + GlobalVariables.index_name
             + " AND  t1.B>t2.B",
             "postab.tid = t2." + GlobalVariables.index_name +
             " AND postab.attr_name ='A' AND t1.A=postab.attr_val AND  t1." +
             GlobalVariables.index_name + " < t2." + GlobalVariables.index_name
             + " AND  t1.B>t2.B",
            "postab.tid = t1." + GlobalVariables.index_name +
             " AND postab.attr_name = 'B' AND postab.attr_val>t2.B AND  t1." +
             GlobalVariables.index_name + " < t2." + GlobalVariables.index_name
             + " AND  t1.A=t2.A",
            "postab.tid = t2." + GlobalVariables.index_name +
             " AND postab.attr_name ='B' AND t1.B>postab.attr_val AND  t1." +
             GlobalVariables.index_name + " < t2." + GlobalVariables.index_name
             + " AND  t1.A=t2.A",
            "postab.tid = t1." + GlobalVariables.index_name +
             " AND postab.attr_name = 'C' AND postab.attr_val>='f' AND  t1." +
             GlobalVariables.index_name + " < t2." + GlobalVariables.index_name
             + " AND  t2.C<='m' AND  t1.E=t2.E",
            "postab.tid = t2." + GlobalVariables.index_name +
             " AND postab.attr_name = 'C' AND postab.attr_val<='m' AND  t1." +
             GlobalVariables.index_name + " < t2." + GlobalVariables.index_name
             + " AND  t1.C>='f' AND  t1.E=t2.E",
            "postab.tid = t1." + GlobalVariables.index_name +
             " AND postab.attr_name = 'E' AND postab.attr_val=t2.E AND  t1." +
             GlobalVariables.index_name + " < t2." + GlobalVariables.index_name
             + " AND  t1.C>='f' AND  t2.C<='m'",
            "postab.tid = t2." + GlobalVariables.index_name +
             " AND postab.attr_name ='E' AND t1.E=postab.attr_val AND  t1." +
             GlobalVariables.index_name + " < t2." + GlobalVariables.index_name
             + " AND  t1.C>='f' AND  t2.C<='m'"
             ]

        self.assertEquals(relaxed_dcs, expected_r_dcs)


if __name__ == "__main__":
    unittest.main()
