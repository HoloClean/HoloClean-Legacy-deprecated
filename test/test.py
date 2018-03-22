import sys
sys.path.append("..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.mysql_dcerrordetector import MysqlDCErrorDetection
import time


class Testing:
    def __init__(self):
        self.holo_obj = HoloClean(
            mysql_driver="../holoclean/lib/mysql-connector-java-5.1.44-bin.jar",
            verbose=True,
            timing_file='execution_time.txt',
            learning_iterations=50,
            learning_rate=0.001,
            batch_size=20)
        self.session = Session(self.holo_obj)

    def test(self):

        t1 = time.time()
        #dataset = "../tutorial/data/hospital_dataset.csv"
        # dataset = "../datasets/flights/flight_input_holo.csv"
        dataset = "../datasets/food/food_input_medium.csv"
        # dataset = "../datasets/unit_test/unit_test_dataset.csv"

        #denial_constraints = "../tutorial/data/hospital_constraints.txt"
        # denial_constraints = "../datasets/flights/flight_constraints.txt"
        denial_constraints = "../datasets/food/food_constraints.txt"
        # denial_constraints = "../datasets/unit_test/unit_test_constraints.txt"

        flattening = 0
        # flattening = 1

        #ground_truth = "../tutorial/data/groundtruth.csv"
        # ground_truth = "../datasets/flights/flights_clean.csv"
        ground_truth = "../datasets/food/food_clean.csv"
        # ground_truth = 0

        # Ingesting Dataset and Denial Constraints
        self.session.load_data(dataset)
        self.session.load_denial_constraints(denial_constraints)

        # Error Detector

        t3 = time.time()
        detector = MysqlDCErrorDetection(self.session)
        self.session.detect_errors(detector)
        t4 = time.time()
        if self.holo_obj.verbose:
            self.holo_obj.logger.info("Error detection time:")
            self.holo_obj.logger.info("Error detection time:" + str(t4-t3))

        self.session.repair()

        if ground_truth:
            self.session.compare_to_truth(ground_truth)

        t2 = time.time()
        if self.holo_obj.verbose:
            self.holo_obj.logger.info("Total time:" + str(t2-t1))
