from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.errordetector import ErrorDetectors
from holoclean.featurization.featurizer import Signal_Init,Signal_cooccur, Signal_dc
from holoclean.learning.accuracy import Accuracy
from time import time as t


class Testing:
    def __init__(self):
        self.holo_obj = HoloClean()
        self.session = Session("Session", self.holo_obj)

    def test(self):
        self.fx = open('execution_time.txt', 'w')
        start_time = t()
        self.session.ingest_dataset("test/test100.csv")
        self.fx.write('ingest csv time: '+str(t()-start_time)+'\n')
        start_time = t()
        self.session.denial_constraints("test/dc100.txt")
        self.fx.write('read denial constraints time: '+str(t()-start_time)+'\n')
        start_time = t()
        err_detector = ErrorDetectors(self.session.Denial_constraints, self.holo_obj.dataengine,
                                      self.holo_obj.spark_session, self.session.dataset)
        self.session.add_error_detector(err_detector)
        self.session.ds_detect_errors()
        self.fx.write('error dectection time: '+str(t()-start_time)+'\n')
        start_time = t()
        pruning_threshold=0
        self.session.ds_domain_pruning(pruning_threshold)
        self.fx.write('domain pruning time: '+str(t()-start_time)+'\n')
        start_time = t()
        start_time1 = t()
        initial_value_signal = Signal_Init(self.session.Denial_constraints, self.holo_obj.dataengine,
                                           self.session.dataset)
        self.fx.write('init signal time: '+str(t()-start_time)+'\n')
        start_time = t()
        self.session.add_featurizer(initial_value_signal)
        statistics_signal = Signal_cooccur(self.session.Denial_constraints, self.holo_obj.dataengine,
                                           self.session.dataset)
        self.fx.write('cooccur signal time: '+str(t()-start_time)+'\n')
        start_time = t()
        self.session.add_featurizer(statistics_signal)
        self.fx.write('dc signal time: '+str(t()-start_time)+'\n')
        start_time = t()
        dc_signal = Signal_dc(self.session.Denial_constraints, self.holo_obj.dataengine, self.session.dataset)
        self.fx.write('dc featurize time: '+str(t()-start_time)+'\n')
        self.session.add_featurizer(dc_signal)
        self.session.ds_featurize()
        self.fx.write('total featurization time: '+str(t()-start_time1)+'\n')
        start_time = t()
        self.session._numskull()
        self.fx.write('numbskull time: '+str(t()-start_time)+'\n')
        start_time = t()
        self.session.ds_repair()
        self.fx.write('repair time: '+str(t()-start_time)+'\n')
        # acc = Accuracy(self.holo_obj.dataengine, "test/gt.csv", self.session.dataset, self.holo_obj.spark_session)
        # acc.accuracy_calculation()
        self.fx.close()


