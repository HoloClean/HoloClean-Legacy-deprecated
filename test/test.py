from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.errordetector import ErrorDetectors
from holoclean.featurization.featurizer import SignalInit, SignalCooccur, SignalDC, SignalSource
from holoclean.learning.softmax import SoftMax
from holoclean.learning.accuracy import Accuracy
import time


class Testing:
    def __init__(self):
        self.holo_obj = HoloClean(mysql_driver="../holoclean/lib/mysql-connector-java-5.1.44-bin.jar")
        self.session = Session("Session", self.holo_obj)

    def test(self):
        self.fx = open('execution_time.txt', 'w')

        # dataset = "../datasets/hospital1k/hospital_dataset.csv"
        dataset = "../datasets/flights/flight_input_holo.csv"
        # dataset = "../datasets/food/food_input_holo.csv"
        # dataset = "../datasets/unit_test/unit_test_dataset.csv"

        # denial_constraints = "../datasets/hospital1k/hospital_constraints.txt"
        denial_constraints = "../datasets/flights/flight_constraints.txt"
        # denial_constraints = "../datasets/food/food_constraints1.txt"
        # denial_constraints = "../datasets/unit_test/unit_test_constraints.txt"

        flattening = 0
        # flattening = 1

        # ground_truth = "../datasets/hospital1k/groundtruth.csv"
        ground_truth = "../datasets/flights/flights_clean.csv"
        # ground_truth = "../datasets/food/food_clean.csv"
        # ground_truth = 0

        # Ingesting Dataset and Denial Constraints
        start_time = time.time()
        t0 = time.time()
        self.session.ingest_dataset(dataset)
        t1 = time.time()
        total = t1 - t0
        self.fx.write('time for ingesting file: ' + str(total) + '\n')
        print 'time for ingesting file: ' + str(total) + '\n'
        self.session.denial_constraints(denial_constraints)

        # Error Detector
        t0 = time.time()
        err_detector = ErrorDetectors(self.session.Denial_constraints, self.holo_obj.dataengine,
                                      self.holo_obj.spark_session, self.session.dataset)
        self.session.add_error_detector(err_detector)
        self.session.ds_detect_errors()
        t1 = time.time()
        total = t1 - t0
        self.holo_obj.logger.info('error dectection time: '+str(total)+'\n')
        self.fx.write('error dectection time: '+str(total)+'\n')
        print 'error dectection time: '+str(total)+'\n'

        # Domain Pruning
        t0 = time.time()
        pruning_threshold = 0.3
        self.session.ds_domain_pruning(pruning_threshold)
        t1 = time.time()
        total = t1 - t0
        self.holo_obj.logger.info('domain pruning time: '+str(total)+'\n')
        self.fx.write('domain pruning time: '+str(total)+'\n')
        print 'domain pruning time: '+str(total)+'\n'

        # Featurization
        t0 = time.time()
        '''initial_value_signal = SignalInit(self.session.Denial_constraints, self.holo_obj.dataengine,
                                          self.session.dataset)
        self.session.add_featurizer(initial_value_signal)
        statistics_signal = SignalCooccur(self.session.Denial_constraints, self.holo_obj.dataengine,
                                          self.session.dataset)
        self.session.add_featurizer(statistics_signal)
        '''
        source_signal = SignalSource(self.session.Denial_constraints, self.holo_obj.dataengine, self.session.dataset,
                         self.holo_obj.spark_session, 1, 0)

        self.session.add_featurizer(source_signal)

        '''dc_signal = SignalDC(self.session.Denial_constraints, self.holo_obj.dataengine, self.session.dataset,
                             self.holo_obj.spark_session)
        self.session.add_featurizer(dc_signal)
        '''
        t1 = time.time()
        total = t1 - t0
        print "Feature Signal Time:", total
        t0 = time.time()
        self.session.ds_featurize()

        t1 = time.time()

        total = t1 - t0

        self.holo_obj.logger.info('featurization time: '+str(total)+'\n')
        self.fx.write('featurization time: '+str(total)+'\n')
        print 'featurization time: '+str(total)+'\n'

        # Learning
        t0 = time.time()
        soft = SoftMax(self.holo_obj.dataengine, self.session.dataset, self.holo_obj,
                       self.session.X_training)

        print(soft.logreg())
        t1 = time.time()
        total = t1 - t0

        self.fx.write('time for training model: '+str(total)+'\n')
        print 'time for training model: '+str(total)+'\n'

        print()

        t0 = time.time()
        self.session.ds_featurize(0)
        t1 = time.time()
        total = t1 - t0
        self.fx.write('time for test featurization: ' + str(total) + '\n')
        print 'time for test featurization: ' + str(total) + '\n'

        #Inference
        t0 = time.time()
        Y = soft.predict(soft.model, self.session.X_testing, soft.setupMask(0, self.session.N,self.session.L))
        t1 = time.time()
        total = t1 - t0
        print 'time for inference: ', total
        t0 = time.time()
        soft.save_prediction(Y)
        t1 = time.time()
        print 'time to save inferred values', t1 - t0

        if ground_truth:
            acc = Accuracy(self.holo_obj.dataengine, ground_truth, self.session.dataset,
                                   self.holo_obj.spark_session)
            acc.accuracy_calculation(flattening)
            self.session.create_corrected_dataset()
        endtime = time.time()
        print 'total time: ', endtime - start_time

        self.fx.close()
