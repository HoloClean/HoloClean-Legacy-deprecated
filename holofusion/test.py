from holofusion import HoloFusion, HoloFusionSession
from time import time as t
import sys
class Testing:
    def __init__(self):
        self.holo_obj = HoloFusion(majority_vote = 1)
        self.session = HoloFusionSession("Session", self.holo_obj)

    def test(self):
        print "Testing started :"+str(t())
        self.fx = open('execution_time.txt', 'w')
        list_time = []
        start_time = t()
        self.session.ingest_dataset("flight-data/flight-data.csv")
        d = t()-start_time
        list_time.append(d)
        self.holo_obj.logger.info('ingest csv time: '+str(d)+'\n')
        self.fx.write('ingest csv time: '+str(d)+'\n')
        print 'ingest csv time: '+str(d)+'\n'
        start_time = t()
        self.fx.close()
        self.session.feature()
        self.session.inference()
        self.session.accuracy("flight-data/flight-data_truth.csv")
        return

test = Testing()
test.test()
