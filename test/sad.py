import unittest
import sys
sys.path.append("..")
from holoclean.holoclean import HoloClean, Session
from holoclean.DCFormatException import DCFormatException



class TestAPI(unittest.TestCase):
    def setUp(self):
        self.holo_obj = HoloClean(
            mysql_driver="../holoclean/lib/mysql-connector-java-5.1.44-bin.jar",
            verbose=True,
            timing_file='execution_time.txt')
        self.session = Session(self.holo_obj)

    def test_add_dc_file_good(self):
        dcs = self.session.load_denial_constraints("../datasets/unit_test/unit_test_constraints.txt")
        self.assertEquals(len(dcs),2)
        self.session.remove_denial_constraint(1)
        self.session.remove_denial_constraint(1)

    def test_add_dc_file_bad_dc(self):
        with self.assertRaises(DCFormatException):
            self.session.add_denial_constraint("t1&t2&EQ(t1.A,t2.A)&IQ(t1.B,t2.B")

    def test_add_dc_file_bad_path(self):
        with self.assertRaises(Exception):
            self.session.load_denial_constraints("../datasets/unit_test/units.txt")

    def test_add_dc_string_good(self):
        dcs = self.session.add_denial_constraint("t1&t2&EQ(t1.B,t2.B)&IQ(t1.C,t2.C)")
        self.assertTrue("t1&t2&EQ(t1.B,t2.B)&IQ(t1.C,t2.C)" in dcs)

    def test_add_dc_string_bad(self):
        with self.assertRaises(DCFormatException):
            self.session.add_denial_constraint("t1t2&EQ(t1.A,t2.A)&IQ(t1.B,t2.B)")


    def test_rm_dc_good(self):
        self.session.load_denial_constraints("../datasets/unit_test/unit_test_constraints.txt")
        dcs = self.session.remove_denial_constraint(1)
        self.assertEquals(len(dcs),1)
        dcs = self.session.remove_denial_constraint(1)
        self.assertEquals(len(dcs), 1)

    def test_rm_dc_bad(self):
        with self.assertRaises(Exception):
            self.session.load_denial_constraints("../datasets/unit_test/unit_test_constraints.txt")
            self.session.remove_denial_constraint(3)


if __name__ == "__main__":
  unittest.main()
