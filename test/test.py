import sys
sys.path.append("..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from holoclean.errordetection.sql_nullerrordetector import\
    SqlnullErrorDetection
import time


class Testing:
    def __init__(self):
        self.holo_obj = HoloClean(
            holoclean_path="..",
            verbose=True,
            pruning_threshold=0.1,
            learning_iterations=20,
            learning_rate=0.001,
            batch_size=20)
        self.session = Session(self.holo_obj)

    def test(self):
        
        t1 = time.time()
        dataset = "../datasets/hospital1k/hospital_dataset_norm.csv"
        # dataset = "../datasets/flights/flight_input_holo.csv"
        #dataset = "../datasets/food/food_input_large_norm.csv"
        # dataset = "../datasets/unit_test/unit_test_dataset.csv"
        # dataset = "../datasets/unit_test/unit_test_one_tuple.csv"

        denial_constraints = "../datasets/hospital1k/hospital_constraints.txt"
        # denial_constraints = "../datasets/flights/flight_constraints.txt"
        #denial_constraints = "../datasets/food/food_constraints.txt"
        # denial_constraints = "../datasets/unit_test/unit_test_constraints.txt"
        # denial_constraints = "../datasets/unit_test/one_tuple_constraints.txt"

        flattening = 0
        # flattening = 1

        ground_truth = "../datasets/hospital1k/groundtruth_norm.csv"
        # ground_truth = "../datasets/flights/flights_clean.csv"
        #ground_truth = "../datasets/food/food_clean.csv"
        # ground_truth = 0

        # Ingesting Dataset and Denial Constraints
        self.session.load_data(dataset)
        self.session.load_denial_constraints(denial_constraints)

        # Error Detector

        t3 = time.time()
        detector_list = []
        Dcdetector = SqlDCErrorDetection(self.session)
        Nulldetector = SqlnullErrorDetection(self.session)
        detector_list.append(Dcdetector)
        detector_list.append(Nulldetector)
        self.session.detect_errors(detector_list)
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
        exit(0)
