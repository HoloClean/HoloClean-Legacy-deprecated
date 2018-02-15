from time import sleep
from threading import Thread, Lock, Condition
import logging
import threading
import time
from holoclean.dataengine import *
from collections import deque

printLock = Lock()
DCqueryCV = Condition()
dc_queries = deque([])

class DatabaseWorker(Thread):
    __lock = Lock()

    def __init__(self,table_name, result_queue, list_of_names, holo_env, dataset, cv):
        Thread.__init__(self)
        self.table_name = table_name
        self.result_queue = result_queue
        self.dataengine = DataEngine(holo_env)
        self.dataset = dataset
        self.list_of_names = list_of_names
        self.cv = cv

    def run(self):
        result = None

        string_name = str(threading.currentThread().getName())
        name_list = list (string_name)
        name_list[6]="_"
        name = "".join(name_list)

        table_name = self.dataset.return_id() + "_" + name + "_" + self.table_name

        self.list_of_names.append(table_name)

        query_for_featurization = "CREATE TABLE " + table_name + "(vid INT, assigned_val INT," \
            " feature INT ,count INT);"
        self.dataengine.query(query_for_featurization)

        while True:
            self.cv.acquire()
            while len(self.result_queue) == 0:
                self.cv.wait()
            self.cv.release()

            list2 = self.result_queue.popleft()
            if list2 == -1:
                break
            insert_signal_query = "INSERT INTO " + table_name + \
                                  " SELECT * FROM " + list2 + ")AS T_0;"
            t0 = time.time()
            printLock.acquire()
            print threading.currentThread().getName(), " Query Started "
            printLock.release()
            self.dataengine.query(insert_signal_query)
            t1= time.time()
            printLock.acquire()
            print threading.currentThread().getName(), " Query Execution time: ", t1-t0
            printLock.release()
        printLock.acquire()
        print threading.currentThread().getName(), " Done executing queries"
        printLock.release()


class QueryProd(Thread):
    __lock = Lock()

    def __init__(self, list_of_queries, clean, feature, cv):
        Thread.__init__(self)
        self.list_of_queries = list_of_queries
        self.clean = clean
        self.feature = feature
        self.cv = cv

    def run(self):
        self.list_of_queries.append(self.feature.get_query(self.clean))
        self.cv.acquire()
        self.cv.notify()
        self.cv.release()


class FeatureProducer(Thread):
    __lock = Lock()

    def __init__(self, clean, cv, list_of_queries, num_of_threads, featurizers):
        Thread.__init__(self)
        self.clean = clean
        self.cv = cv
        self.list_of_queries = list_of_queries
        self.num_of_threads = num_of_threads
        self.featurizers = featurizers

    def run(self):
        prods = []
        for feature in self.featurizers:
            printLock.acquire()
            print 'adding a ', feature.id
            printLock.release()
            t0 = time.time()
            if feature.id != "SignalDC":
                thread = QueryProd(self.list_of_queries, self.clean, feature, self.cv)
                prods.append(thread)
                thread.start()
            t1 = time.time()
            total = t1 - t0
            printLock.acquire()
            print 'done adding ', feature.id, ' ', total
            printLock.release()

        global dc_queries
        while True:
            DCqueryCV.acquire()
            while len(dc_queries) == 0:
                DCqueryCV.wait()
            DCqueryCV.release()

            dc_query = dc_queries.popleft()
            if dc_query == -1:
                break
            printLock.acquire()
            print 'adding a DC query'
            printLock.release()
            self.list_of_queries.append(dc_query)
            self.cv.acquire()
            self.cv.notify()
            self.cv.release()

            printLock.acquire()
            print 'finished adding a DC query'
            printLock.release()

        for thread in prods:
            thread.join()
        self.cv.acquire()
        for i in range(self.num_of_threads):
            self.list_of_queries.append(-1)
            self.cv.notify()
        self.cv.release()

        printLock.acquire()
        print 'Feature Prod done'
        printLock.release()


class DCQueryProducer(Thread):
    __lock = Lock()

    def __init__(self, clean, featurizers):
        Thread.__init__(self)
        self.clean = clean
        self.featurizers = featurizers

    def run(self):
        clean = self.clean
        global dc_queries

        for feature in self.featurizers:
            if feature.id == "SignalDC":
                feature.get_query(clean, self)
        DCqueryCV.acquire()
        dc_queries.append(-1)
        DCqueryCV.notify()
        DCqueryCV.release()

        printLock.acquire()
        print 'DC QUERY Producer FINISHED'
        printLock.release()

    def appendQuery(self, query):
        dc_queries.append(query)
        DCqueryCV.acquire()
        DCqueryCV.notify()
        DCqueryCV.release()

'''
worker1 = DatabaseWorker("db1", "select something from sometable",
        result_queue)
worker2 = DatabaseWorker("db1", "select something from othertable",
        result_queue)
worker1.start()
worker2.start()

# Wait for the job to be done
while len(result_queue) < 2:
    sleep(delay)
job_done = True
worker1.join()
worker2.join()
'''