from holofusion import HoloFusion, HoloFusionSession
from time import time as t


class Testing:
    def __init__(self):
        self.holo_obj = HoloFusion(majority_vote=0, training_data=1)
        self.session = HoloFusionSession("Session", self.holo_obj)
        self.fx = open('execution_time.txt', 'w')

    def test(self):
        print "Testing started :"+str(t())
        list_time = []
        start_time = t()

        self.session.ingest_dataset("data/stock-data/stock-data-test.csv")
        d = t()-start_time
        list_time.append(d)
        self.fx.write('ingest csv time: '+str(d)+'\n')
        print 'ingest csv time: '+str(d)+'\n'

        start_time = t()
        self.session.adding_training_data("data/stock-data/stock-data_training.csv")
        d = t() - start_time
        list_time.append(d)
        self.fx.write('adding training data: ' + str(d) + '\n')
        print 'adding training time: ' + str(d) + '\n'

        start_time = t()
        self.session.feature()
        d = t() - start_time
        list_time.append(d)
        self.fx.write('creating feature table time: '+str(d)+'\n')
        print ' feature table time: '+str(d)+'\n'

        start_time = t()
        self.session.inference()
        d = t() - start_time
        list_time.append(d)
        self.fx.write('inference time: ' + str(d) + '\n')
        print 'inference time: ' + str(d) + '\n'
        start_time = t()

        self.session.accuracy("data/stock-data/stock-data_truth.csv")
        d = t() - start_time
        list_time.append(d)
        self.fx.write('time to calculate accuracy: ' + str(d) + '\n')
        print 'time to calculate accuracy: ' + str(d) + '\n'
        self.fx.close()
        return


test = Testing()
test.test()
