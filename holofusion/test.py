from holofusion import HoloFusion, HoloFusionSession
from time import time as t


class Testing:
    def __init__(self):
        self.holo_obj = HoloFusion()
        self.session = HoloFusionSession("Session", self.holo_obj)

    def test(self):
        print "Testing started :"+str(t())
        self.fx = open('execution_time.txt', 'w')
        list_time = []
        start_time = t()
        self.session.ingest_dataset("book-data/book.csv")
        d = t()-start_time
        list_time.append(d)
        self.holo_obj.logger.info('ingest csv time: '+str(d)+'\n')
        self.fx.write('ingest csv time: '+str(d)+'\n')
        print 'ingest csv time: '+str(d)+'\n'
        start_time = t()
        self.fx.close()
        self.session.feature()
        self.session.inference()


test = Testing()
test.test()
