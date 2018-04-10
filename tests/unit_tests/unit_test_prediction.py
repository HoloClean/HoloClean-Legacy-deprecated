import unittest
import sys
sys.path.append("../..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from holoclean.errordetection.sql_nullerrordetector import SqlnullErrorDetection

k_inferred = 2
holo_obj = HoloClean(
    holoclean_path="../..",
    verbose=False,
    pruning_threshold1=0.001,
    pruning_clean_breakoff=6,
    pruning_threshold2=0.0,
    pruning_dk_breakoff=6,
    learning_iterations=30,
    learning_rate=0.001,
    batch_size=5,
    k_inferred=k_inferred)

session = Session(holo_obj)
dataset = "../data/hospital.csv"
session.load_data(dataset)

session.load_denial_constraints(
    "../data/hospital_constraints.txt")
detector_list = []
Dcdetector = SqlDCErrorDetection(session)
Nulldetector = SqlnullErrorDetection(session)
detector_list.append(Dcdetector)
detector_list.append(Nulldetector)
session.detect_errors(detector_list)
session.repair()

class UnitTestPredictions(unittest.TestCase):

    def setUp(self):
        pass

    def test_table_size(self):
        # test that the size of Inferred map is a subset of Inferred values
        self.assertEquals(
            session.inferred_map.subtract(session.inferred_values).count(),
            0
        )

        self.assertGreaterEqual(
            session.inferred_values.subtract(session.inferred_map).count(),
            0
        )
        pass

    def test_content(self):
        # test for content of Inferred values and Inferred map
        df_list = []
        # Insert with dummy probability
        df_list.append([0.5, 1, 'City', 'birmingham', 1, 5])
        # create expected df for inferred map
        df_test = session.holo_env.spark_session.createDataFrame(df_list, session.dataset.attributes['Inferred_values'])
        self.assertEqual(
            session.inferred_map.drop('probability').filter("vid = 1").intersect(df_test.drop('probability')).count(),
            1
        )

        # Do same for inferred_values
        df_list.append([0.5, 1, 'City', 'montgomery', 1, 4])
        df_test = session.holo_env.spark_session.createDataFrame(df_list, session.dataset.attributes['Inferred_values'])
        self.assertEqual(
            session.inferred_values.drop('probability').filter("vid = 1").intersect(df_test.drop('probability')).count(),
            2
        )

    def test_k(self):
        # test the max amount of rows for each vid in Inferred values is less than k
        query = "SELECT MAX(c) as m FROM" \
                " (SELECT count(*) as c from " + session.dataset.table_specific_name('Inferred_values') + \
                "  Group by vid) t"
        max_count = session.holo_env.dataengine.query(query, 1).collect()[0]['m']
        self.assertLessEqual(max_count, k_inferred)


if __name__ == "__main__":
    unittest.main()