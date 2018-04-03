from threading import Thread, Lock, Condition
from holoclean.dataengine import DataEngine
import threading
import time
from collections import deque

printLock = Lock()


class DatabaseWorker(Thread):
    """
    DatabaseWorker will retrieve queries to run from queries and execute them
    until a -1 is received
    """
    __lock = Lock()

    table_names = []
    queries = deque([])

    def __init__(self, session):
        Thread.__init__(self)
        self.session = session
        self.holo_env = self.session.holo_env
        self.dataengine = DataEngine(self.holo_env)
        self.dataset = self.session.dataset
        self.exit_code = 0

    def run(self):

        # Get the table name of this thread
        string_name = str(threading.currentThread().getName())
        name_list = list(string_name)
        name_list[6] = "_"
        name = "".join(name_list)
        queries = DatabaseWorker.queries
        try:
            table_name = self.dataset.table_specific_name(name)
            DatabaseWorker.table_names.append(table_name)

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
                DatabaseWorker.__lock.acquire()
                if len(queries) == 0:
                    DatabaseWorker.__lock.release()
                    break
                DatabaseWorker.__lock.release()
                query = queries.popleft()
                insert_signal_query = "INSERT INTO " + table_name + \
                                      "(" + query + ");"

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
            self.dataengine.db_backend[1].close()
        except Exception as e:
            print ("Exception in Thread: " + string_name)
            self.holo_env.logger.info("Exception in Thread: " + string_name)
            self.holo_env.logger.info(str(e))
            self.exit_code = 1
            exit(1)


class PopulateTensor(Thread):
    __lock = Lock()

    def __init__(self, table_name, X_tensor, session):
        Thread.__init__(self)
        self.table_name = table_name
        self.X = X_tensor
        self.holo_env = session.holo_env
        self.dataengine = DataEngine(self.holo_env)

    def run(self):
        table_name = self.table_name
        try:
            if self.X is None:
                raise Exception("X tensor does not exist yet")

            query = "SELECT * FROM " + table_name
            feature_table = self.dataengine.query(query, 1).collect()
            for factor in feature_table:
                self.X[factor.vid - 1, factor.feature - 1,
                       factor.assigned_val - 1] = factor['count']
            if self.holo_env.verbose:
                printLock.acquire()
                msg = str(threading.currentThread().getName()) + \
                      " Done executing queries"
                self.holo_env.logger.info(msg)
                printLock.release()
            drop_table = "DROP TABLE " + table_name
            self.dataengine.query(drop_table)
            self.dataengine.db_backend[1].close()
            exit(0)

        except Exception as e:
            print ("Exception in Thread: " + table_name)
            self.holo_env.logger.info("Exception in Thread: " + table_name)
            self.holo_env.logger.info(str(e))
            self.exit_code = 1
            exit(1)


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
