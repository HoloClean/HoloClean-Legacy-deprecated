from threading import Thread, Lock, Condition
import threading
import time
from collections import deque

printLock = Lock()
query_cv = Condition()
queries = deque([])


class DatabaseWorker(Thread):
    """
    DatabaseWorker will retrieve queries to run from queries and execute them
    until a -1 is received
    """
    __lock = Lock()

    cv = Condition()

    def __init__(self, session, barrier, cvX):
        Thread.__init__(self)
        self.session = session
        self.holo_env = self.session.holo_env
        self.dataengine = self.holo_env.dataengine
        self.dataset = self.session.dataset
        self.barrier = barrier
        self.X = None
        self.cvX = cvX

    def getX(self, X):
        self.X = X

    def run(self):
        # Get the table name of this thread
        string_name = str(threading.currentThread().getName())
        name_list = list(string_name)
        name_list[6] = "_"
        name = "".join(name_list)

        table_name = self.dataset.return_id() +\
            "_" + name
        query_for_featurization = "CREATE TABLE " + table_name + \
                                  "(vid INT, assigned_val INT," \
                                  " feature INT ,count INT);"
        self.dataengine.query(query_for_featurization)
        if self.holo_env.verbose:
            printLock.acquire()
            msg = str(threading.currentThread().getName()) +\
                " has created the table: " +\
                table_name
            self.holo_env.logger.info(msg)
            self.holo_env.logger.info("  ")
            printLock.release()

        while True:
            query_cv.acquire()
            while len(queries) == 0:
                query_cv.wait()

            query = queries.popleft()
            query_cv.release()

            if query == -1:
                break
            insert_signal_query = "INSERT INTO " + table_name + "(" + query + ");"

            start_time = time.time()
            if self.holo_env.verbose:
                printLock.acquire()
                msg = str(threading.currentThread().getName()) +\
                    " Query Started "
                self.holo_env.logger.info(msg)
                print insert_signal_query
                printLock.release()
            self.dataengine.query(insert_signal_query)

            end_time = time.time()

            if self.holo_env.verbose:
                printLock.acquire()
                self.holo_env.logger.info(
                    str(threading.currentThread().getName()) +
                    " Query Execution time: " + str(end_time - start_time))
                self.holo_env.logger.info(str(insert_signal_query))
                self.holo_env.logger.info("  ")
                printLock.release()

        self.barrier.wait()

        self.cvX.acquire()
        self.cvX.wait()
        self.cvX.release()

        if self.X is None:
            raise Exception("X tensor does not exist yet")

        query = "SELECT * FROM " + table_name
        feature_table = self.dataengine.query(query, 1).collect()
        for factor in feature_table:
            self.X[factor.vid - 1, factor.feature - 1,
                   factor.assigned_val - 1] = factor['count']
        if self.holo_env.verbose:
            printLock.acquire()
            msg = str(threading.currentThread().getName()) +\
                " Done executing queries"
            self.holo_env.logger.info(msg)
            printLock.release()
        drop_table = "DROP TABLE " + table_name
        self.dataengine.query(drop_table)


class QueryProducer(Thread):
    """
    QueryProducer will loop through the featurizers given from session
    To append all queries produced by featurizers into a List that
    Database worker will retrieve from
    """
    __lock = Lock()

    def __init__(self, featurizers, clean, num_of_threads):
        Thread.__init__(self)
        self.clean = clean
        self.featurizers = featurizers
        self.num_of_threads = num_of_threads

    def run(self):
        global queries

        # Loop through featurizers to append queries to shared list
        for featurizer in self.featurizers:
            queries_to_add = featurizer.get_query(self.clean)
            for query in queries_to_add:
                query_cv.acquire()
                queries.append(query)
                query_cv.notify()
                query_cv.release()

        # Send -1 to shared list to indicated end
        for i in range(self.num_of_threads):
            query_cv.acquire()
            queries.append(-1)
            query_cv.notify()
            query_cv.release()


class RunQuery(Thread):
    """
    RunQuery will just take in a query to run on a separate Thread
    """
    __lock = Lock()

    def __init__(self, query, session):
        Thread.__init__(self)
        self.query = query
        self.dataengine = session.holo_env.dataengine

    def run(self):
        self.dataengine.query(self.query)
