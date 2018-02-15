from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.errordetector import ErrorDetectors
from holoclean.featurization.featurizer import SignalInit, SignalCooccur, SignalDC
from holoclean.featurization.featurizer import Featurizer
from holoclean.learning.softmax import SoftMax
from holoclean.learning.accuracy import Accuracy
import time

class Testing:
    def __init__(self):
        self.holo_obj = HoloClean()
        self.session = Session("Session", self.holo_obj)

    def test(self):
        self.fx = open('execution_time.txt', 'w')
       # list_time = []
        start_time = time.time()
        t0 = time.time()
        #self.session.ingest_dataset("test/inputDatabase.csv")
        self.session.ingest_dataset("test/test.csv")
        # self.session.ingest_dataset("test/test1.csv")

        t1 = time.time()

        total = t1 - t0
        self.fx.write('time for ingesting file: ' + str(total) + '\n')
        print 'time for ingesting file: ' + str(total) + '\n'

        #self.session.denial_constraints("test/inputConstraint.txt")
        self.session.denial_constraints("test/dc1.txt")
        # self.session.denial_constraints("test/dc2.txt")

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

        t0 = time.time()
        pruning_threshold = 0.5
        self.session.ds_domain_pruning(pruning_threshold)

        t1 = time.time()
        total = t1 - t0
        self.holo_obj.logger.info('domain pruning time: '+str(total)+'\n')
        self.fx.write('domain pruning time: '+str(total)+'\n')
        print 'domain pruning time: '+str(total)+'\n'


        t0 = time.time()
        initial_value_signal = SignalInit(self.session.Denial_constraints, self.holo_obj.dataengine,
                                          self.session.dataset)
        self.session.add_featurizer(initial_value_signal )
        statistics_signal = SignalCooccur(self.session.Denial_constraints, self.holo_obj.dataengine,
                                          self.session.dataset )
        self.session.add_featurizer(statistics_signal)
        dc_signal = SignalDC(self.session.Denial_constraints, self.holo_obj.dataengine, self.session.dataset,
                             self.holo_obj.spark_session)
        self.session.add_featurizer(dc_signal)
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

        t0 = time.time()
        soft = SoftMax(self.holo_obj.dataengine, self.session.dataset, self.holo_obj.spark_session)

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

        t0 = time.time()
        Y = soft.predict(soft.model, soft.setuptrainingX(), soft.setupMask(0))
        print(Y)
        t1 = time.time()
        total = t1 - t0
        print 'time for inference: ', total
        t0 = time.time()
        soft.save_Y_to_db(Y)
        t1 = time.time()
        print 'time to save inferred values', t1 - t0

        '''acc = Accuracy(self.holo_obj.dataengine, "test/hospital1k/grandtruth.csv", self.session.dataset, self.holo_obj.spark_session)
        flattening = 0
        acc.accuracy_calculation(flattening)'''

        '''acc = Accuracy(self.holo_obj.dataengine, "test/correct.csv", self.session.dataset, self.holo_obj.spark_session)
        flattening=1
        acc.accuracy_calculation(flattening)
        '''
        endtime = time.time()
        print 'total time: ', endtime - start_time

        '''start_time = t()
        self.session._numskull()
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('numbskull time: '+str(d)+'\n')
        self.fx.write('numbskull time: '+str(d)+'\n')
        print 'numbskull time: '+str(d)+'\n'
        start_time = t()
        self.session.ds_repair()
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('repair time: '+str(d)+'\n')
        self.fx.write('repair time: '+str(d)+'\n')
        print 'repair time: '+str(d)+'\n'
        # acc = Accuracy(self.holo_obj.dataengine, "test/gt.csv", self.session.dataset, self.holo_obj.spark_session)
        # acc.accuracy_calculation()
        '''


        self.fx.close()
