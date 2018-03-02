import sys
sys.path.append("..")

from holoclean.holoclean import HoloClean, Session
import time


class Testing:
    def __init__(self):
        self.holo_obj = HoloClean(
            mysql_driver="../holoclean/lib/mysql-connector-java-5.1.44-bin.jar",
            verbose=True)
        self.session = Session(self.holo_obj)

    def test(self):
        self.fx = open('execution_time.txt', 'w')

        dataset = "../tutorial/data/hospital_dataset.csv"
        #dataset = "../datasets/flights/flight_input_holo.csv"
        # dataset = "../datasets/food/food_input_holo.csv"
        #dataset = "../datasets/unit_test/unit_test_dataset.csv"

        denial_constraints = "../tutorial/data/hospital_constraints.txt"
        #denial_constraints = "../datasets/flights/flight_constraints.txt"
        # denial_constraints = "../datasets/food/food_constraints1.txt"
        #denial_constraints = "../datasets/unit_test/unit_test_constraints.txt"

        flattening = 0
        # flattening = 1

        ground_truth = "../tutorial/data/groundtruth.csv"
        #ground_truth = "../datasets/flights/flights_clean.csv"
        # ground_truth = "../datasets/food/food_clean.csv"
        #ground_truth = 0

        # Ingesting Dataset and Denial Constraints
        start_time = time.time()
        t0 = time.time()
        self.session.load_data(dataset)
        t1 = time.time()
        total = t1 - t0
        self.fx.write('time for ingesting file: ' + str(total) + '\n')
        print 'time for ingesting file: ' + str(total) + '\n'
        self.session.load_denial_constraints(denial_constraints)

        # Error Detector
        t0 = time.time()
        self.session.detect_errors()
        t1 = time.time()
        total = t1 - t0
        self.holo_obj.logger.info('error dectection time: '+str(total)+'\n')
        self.fx.write('error dectection time: '+str(total)+'\n')
        print 'error dectection time: '+str(total)+'\n'

        self.session.repair()

        if ground_truth:
            self.session.compare_to_truth(ground_truth)
        endtime = time.time()
        print 'total time: ', endtime - start_time

        self.fx.close()
