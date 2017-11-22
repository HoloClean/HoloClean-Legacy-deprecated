from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.errordetector import ErrorDetectors
from holoclean.featurization.featurizer import Signal_Init,Signal_cooccur, Signal_dc
from holoclean.learning.accuracy import Accuracy


class Testing:
    def __init__(self):
        self.holo_obj = HoloClean()
        self.session = Session("Session", self.holo_obj)

    def test(self):
        self.session.ingest_dataset("test/50.csv")
        self.session.denial_constraints("test/dcf.txt")
        err_detector = ErrorDetectors(self.session.Denial_constraints, self.holo_obj.dataengine,
                                      self.holo_obj.spark_session, self.session.dataset)
        self.session.add_error_detector(err_detector)
        self.session.ds_detect_errors()
        self.session.ds_domain_pruning()
        initial_value_signal = Signal_Init(self.session.Denial_constraints, self.holo_obj.dataengine,
                                           self.session.dataset)
        self.session.add_featurizer(initial_value_signal)
        statistics_signal = Signal_cooccur(self.session.Denial_constraints, self.holo_obj.dataengine,
                                           self.session.dataset)
        self.session.add_featurizer(statistics_signal)
        dc_signal = Signal_dc(self.session.Denial_constraints, self.holo_obj.dataengine, self.session.dataset)
        self.session.add_featurizer(dc_signal)
        self.session.ds_featurize()
        self.session._numskull()
        self.session.ds_repair()
        acc = Accuracy(self.holo_obj.dataengine, "test/50groundTruth.csv", self.session.dataset, self.holo_obj.spark_session)
        acc.accuracy_calculation()


