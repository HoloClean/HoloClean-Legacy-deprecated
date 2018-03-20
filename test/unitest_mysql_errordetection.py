import unittest
import sys
sys.path.append("..")
from holoclean.holoclean import HoloClean, Session
from holoclean.DCFormatException import DCFormatException



class TestMysql_error_detector(unittest.TestCase):
    def setUp(self):
        self.holo_obj = HoloClean(
            mysql_driver="../holoclean/lib/mysql-connector-java-5.1.44-bin.jar",
            verbose=True,
            timing_file='execution_time.txt')
        self.session = Session(self.holo_obj)
        dcs = self.session.load_denial_constraints("../datasets/unit_test/unit_test_constraints.txt")

    def test_small_dataset(self):
        detector = Mysql_DCErrorDetection(self.session.Denial_constraints,
                                    self.holo_obj,
                                    self.session.dataset)
	self.session.detect_errors(detector)
        dk = self.holo_obj.dataengine.get_table_to_dataframe(
            'C_dk', self.dataset)
        row_number = dk.count()
        self.assertEquals(row_number, 1)



if __name__ == "__main__":
   unittest.main()
