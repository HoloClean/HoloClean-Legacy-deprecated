from time import sleep
from threading import Thread, Lock
import logging
import threading
import time


class DatabaseWorker(Thread):
    __lock = Lock()

    def __init__(self,table_name, result_queue, list_of_names, dataengine, dataset):
        Thread.__init__(self)
        self.table_name = table_name
        self.result_queue = result_queue
        self.dataengine = dataengine
        self.dataset = dataset
        self.list_of_names = list_of_names

    def run(self):
        result = None

        string_name = str(threading.currentThread().getName())
        name_list = list (string_name)
        name_list[6]="_"
        name = "".join(name_list)

        table_name = self.dataset.return_id() + "_" + name + "_" + self.table_name

        self.list_of_names.append(table_name)

        query_for_featurization = "CREATE TABLE " + table_name + "(vid INT, assigned_val INT," \
            " feature TEXT,count INT);"
        self.dataengine.query(query_for_featurization)

        while True:
            try:
                list2 = self.result_queue.pop()
                print threading.currentThread().getName()
                print list2
                insert_signal_query = "INSERT INTO " + table_name + \
                                      " SELECT * FROM " + list2 + ")AS T_0;"
                self.dataengine.query(insert_signal_query)

               # time.sleep(10)
            except IndexError:
                break  # Done.



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