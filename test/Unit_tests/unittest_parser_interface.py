import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.DCFormatException import DCFormatException

holo_obj = HoloClean(
    holoclean_path='../..',
    verbose=True,
    timing_file='execution_time.txt')


class TestParserInterface(unittest.TestCase):

    def setUp(self):
        self.session = Session(holo_obj)
        self.dataset = "../../datasets/unit_test/unit_test_dataset.csv"
        self.session.load_data(self.dataset)

    def test_load_denial_constraints(self):
        dcs = self.session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_constraints.txt")
        expected = ['t1&t2&EQ(t1.A,t2.A)&IQ(t1.B,t2.B)',
                    't1&t2&EQ(t1.C,"f")&EQ(t2.C,"m")&EQ(t1.E,t2.E)']
        self.assertEqual(dcs, expected)

    def test_check_dc_format(self):
        dcs = self.session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_constraints.txt")

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
        dcs = self.session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_constraints.txt")
        expected = ['t1.A=t2.A AND t1.B<>t2.B',
                    't1.C="f" AND t2.C="m" AND t1.E=t2.E']
        self.assertEqual(self.session.parser.get_CNF_of_dcs(dcs), expected)

    def test_create_dc_map(self):
        dcs = self.session.load_denial_constraints(
            "../../datasets/unit_test/unit_test_constraints.txt"
        )
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
        self.assertEqual(self.session.parser.create_dc_map(dcs), expected)


if __name__ == "__main__":
    unittest.main()
