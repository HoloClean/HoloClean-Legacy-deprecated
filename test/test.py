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
            holoclean_path="..",         # path to holoclean package
            verbose=True,
            # to limit possible values for training data
            pruning_threshold1=0.1,
            # to limit possible values for training data to less than k values
            pruning_clean_breakoff=20,
            # to limit possible values for dirty data
            pruning_threshold2=0.3,
            # to limit possible values for dirty data to less than k values
            pruning_dk_breakoff=10,
            # learning parameters
            learning_iterations=10,
            learning_rate=0.001,
            batch_size=500
        )
        self.session = Session(self.holo_obj)

    def test(self):
        
        t1 = time.time()
        # dataset = "../datasets/hospital1k/hospital.csv"
        dataset = "../datasets/food/food_input_large_transform.csv"

        # denial_constraints =
        # "../datasets/hospital1k/hospital_constraints.txt"
        denial_constraints = "../datasets/food/food_constraints.txt"

        # ground_truth = "../datasets/hospital1k/hospital_clean.csv"
        ground_truth = "../datasets/food/food_clean_norm.csv"

        # uncheck this if you dont have ground truth
        # ground_truth = 0

        # Ingesting Dataset and Denial Constraints
        self.session.load_data(dataset)
        self.session.load_denial_constraints(denial_constraints)

        # Error Detectors: We have two, dc violations and null values

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
            print "Execution finished"

        exit(0)
