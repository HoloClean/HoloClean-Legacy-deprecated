from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.errordetector import ErrorDetectors
from holoclean.featurization.featurizer import SignalInit, SignalCooccur, SignalDC
from holoclean.learning.accuracy import Accuracy
from time import time as t


class Testing:
    def __init__(self):
        self.holo_obj = HoloClean()
        self.session = Session("Session", self.holo_obj)

    def test(self):
        print "Testing started :"+str(t())
        self.fx = open('execution_time.txt', 'w')
        list_time = []
        start_time = t()
        self.session.ingest_dataset("test/inputDatabase.csv")
        #self.session.ingest_dataset("test/test.csv")
        # self.session.ingest_dataset("test/test1.csv")
        d = t()-start_time
        list_time.append(d)
        self.holo_obj.logger.info('ingest csv time: '+str(d)+'\n')
        self.fx.write('ingest csv time: '+str(d)+'\n')
        print 'ingest csv time: '+str(d)+'\n'
        start_time = t()
        self.session.denial_constraints("test/inputConstraint.txt")
        #self.session.denial_constraints("test/dc1.txt")
        # self.session.denial_constraints("test/dc2.txt")
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('read denial constraints time: '+str(d)+'\n')
        self.fx.write('read denial constraints time: '+str(d)+'\n')
        print 'read denial constraints time: '+str(d)+'\n'
        start_time = t()
        err_detector = ErrorDetectors(self.session.Denial_constraints, self.holo_obj.dataengine,
                                      self.holo_obj.spark_session, self.session.dataset)
        self.session.add_error_detector(err_detector)
        self.session.ds_detect_errors()
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('error dectection time: '+str(d)+'\n')
        self.fx.write('error dectection time: '+str(d)+'\n')
        print 'error dectection time: '+str(d)+'\n'
        start_time = t()
        pruning_threshold = 0.5
        self.session.ds_domain_pruning(pruning_threshold)
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('domain pruning time: '+str(d)+'\n')
        self.fx.write('domain pruning time: '+str(d)+'\n')
        print 'domain pruning time: '+str(d)+'\n'
        start_time = t()
        start_time1 = t()
        initial_value_signal = SignalInit(self.session.Denial_constraints, self.holo_obj.dataengine,
                                          self.session.dataset)
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('init signal time: '+str(d)+'\n')
        self.fx.write('init signal time: '+str(d)+'\n')
        print 'init signal time: '+str(d)+'\n'
        start_time = t()
        self.session.add_featurizer(initial_value_signal)
        statistics_signal = SignalCooccur(self.session.Denial_constraints, self.holo_obj.dataengine,
                                          self.session.dataset)
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('cooccur signal time: '+str(d)+'\n')
        self.fx.write('cooccur signal time: '+str(d)+'\n')
        print 'cooccur signal time: '+str(d)+'\n'
        start_time = t()
        self.session.add_featurizer(statistics_signal)
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('dc signal time: '+str(d)+'\n')
        self.fx.write('dc signal time: '+str(d)+'\n')
        print 'dc signal time: '+str(d)+'\n'
        start_time = t()
        dc_signal = SignalDC(self.session.Denial_constraints, self.holo_obj.dataengine, self.session.dataset)
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('dc featurize time: '+str(d)+'\n')
        self.fx.write('dc featurize time: '+str(d)+'\n')
        print 'dc featurize time: '+str(d)+'\n'
        self.session.add_featurizer(dc_signal)
        self.session.ds_featurize()
        d = t() - start_time
        list_time.append(d)
        self.holo_obj.logger.info('total featurization time: '+str(d)+'\n')
        self.fx.write('total featurization time: '+str(d)+'\n')
        print 'total featurization time: '+str(d)+'\n'
        start_time = t()
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

        self.holo_obj.logger.info('Total time: ' + str(sum(list_time)) + '\n')
        self.fx.write('Total time: ' + str(sum(list_time)) + '\n')
        print 'Total time: ' + str(sum(list_time)) + '\n'

        self.fx.close()
