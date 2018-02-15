#!/usr/bin/env python


import logging
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time

from dataengine import DataEngine
from dataset import Dataset
from featurization.featurizer import Featurizer
from featurization.DatabaseWorker import DatabaseWorker, FeatureProducer, DCQueryProducer
from learning.inference import inference
from learning.wrapper import Wrapper
from utils.pruning import Pruning
from learning.softmax import SoftMax
from threading import Thread, Lock, Condition
import multiprocessing

# Define arguments for HoloClean
arguments = [
    (('-u', '--db_user'),
        {'metavar': 'DB_USER',
         'dest': 'db_user',
         'default': 'holocleanUser',
         'type': str,
         'help': 'User for DB used to persist state'}),
    (('-p', '--password', '--pass'),
        {'metavar': 'PASSWORD',
         'dest': 'db_pwd',
         'default': 'abcd1234',
         'type': str,
         'help': 'Password for DB used to persist state'}),
    (('-h', '--host'),
        {'metavar': 'HOST',
         'dest': 'db_host',
         'default': 'localhost',
         'type': str,
         'help': 'Host for DB used to persist state'}),
    (('-d', '--database'),
        {'metavar': 'DATABASE',
         'dest': 'db_name',
         'default': 'holo',
         'type': str,
         'help': 'Name of DB used to persist state'}),
    (('-m', '--mysql_driver'),
        {'metavar': 'MYSQL_DRIVER',
         'dest': 'mysql_driver',
         'default': 'holoclean/lib/mysql-connector-java-5.1.44-bin.jar',
         'type': str,
         'help': 'Path for MySQL driver'}),
    (('-s', '--spark_cluster'),
        {'metavar': 'SPARK',
         'dest': 'spark_cluster',
         'default': None,
         'type': str,
         'help': 'Spark cluster address'}),
    (('-t', '--threshold'),
     {'metavar': 'THRESHOLD',
      'dest': 'threshold',
      'default': 0,
      'type': float,
      'help': 'The threshold of the probabilities which are shown in the results'}),
    (('-k', '--first_k'),
     {'metavar': 'FIRST_K',
      'dest': 'first_k',
      'default': 1,
      'type': int,
      'help': 'The final output will show the k-first results (if it is 0 it will show everything)'}),
]


flags = [
    (('-q', '--quiet'),
        {'default': False,
         'dest': 'quiet',
         'action': 'store_true',
         'help': 'quiet'}),
    (tuple(['--verbose']),
        {'default': False,
         'dest': 'verbose',
         'action': 'store_true',
         'help': 'verbose'})
]


flags = [
    (('-q', '--quiet'),
        {'default': False,
         'dest': 'quiet',
         'action': 'store_true',
         'help': 'quiet'}),
    (tuple(['--verbose']),
        {'default': False,
         'dest': 'verbose',
         'action': 'store_true',
         'help': 'verbose'})
]


class HoloClean:
    """TODO.
    Main Entry Point for HoloClean.
    Creates a HoloClean Data Engine
    and initializes Spark.
    """

    def __init__(self, **kwargs):
        """TODO.

        Parameters
        ----------
        parameter : type
           This is a parameter

        Returns
        -------
        describe : type
            Explanation
        """

        # Initialize default execution arguments
        arg_defaults = {}
        for arg, opts in arguments:
            if 'directory' in arg[0]:
                arg_defaults['directory'] = opts['default']
            else:
                arg_defaults[opts['dest']] = opts['default']

        # Initialize default execution flags
        for arg, opts in flags:
            arg_defaults[opts['dest']] = opts['default']

        for key in kwargs:
            arg_defaults[key] = kwargs[key]

        # Initialize additional arguments
        for (arg, default) in arg_defaults.items():
            setattr(self, arg, kwargs.get(arg, default))

        logging.basicConfig(filename="logger.log",
                            filemode='w', level=logging.ERROR)
        self.logger = logging.getLogger("__main__")
        logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
        # Initialize dataengine and spark session

        self.spark_session, self.spark_sql_ctxt = self._init_spark()
        self.dataengine = self._init_dataengine()

        # Init empty session collection
        self.session = {}
        self.session_id = 0

    # Internal methods
    def _init_dataengine(self):
        """TODO: Initialize HoloClean's Data Engine"""
        # if self.dataengine:
        #    return
        data_engine = DataEngine(self)
        return data_engine

    def _init_spark(self):
        """TODO: Initialize Spark Session"""
        # Set spark configuration
        conf = SparkConf()
        # Link MySQL driver to Spark Engine
        conf.set("spark.executor.extraClassPath", self.mysql_driver)
        conf.set("spark.driver.extraClassPath", self.mysql_driver)
        conf.set('spark.driver.memory', '20g')
        conf.set('spark.executor.memory', '20g')
        conf.set("spark.network.timeout", "6000")
        conf.set("spark.rpc.askTimeout", "99999")
        conf.set("spark.worker.timeout", "60000")
        conf.set("spark.driver.maxResultSize", '70g')
        conf.set("spark.ui.showConsoleProgress", "false")

        if self.spark_cluster:
            conf.set("spark.master", self.spark_cluster)

        # Get Spark context
        sc = SparkContext(conf=conf)
        sc.setLogLevel("OFF")
        sql_ctxt = SQLContext(sc)
        return sql_ctxt.sparkSession, sql_ctxt

    # Setters
    def set_dataengine(self, new_dataengine):
        """TODO: Manually set Data Engine"""
        self.dataengine = new_dataengine
        return

    # Getters
    def get_spark_session(self):
        """TODO: Get spark session"""
        return self.spark_session

    def get_spark_sql_context(self):
        """TODO: Get Spark SQL context"""
        return self.spark_sql_ctxt

    def start_new_session(self, name='session'):
        """TODO: Get new HoloClean Session"""
        newSession = Session(name + str(self.session_id))
        self.section_id += 1
        return newSession

    def get_session(self, name):
        if name in self.session:
            return self.session[name]
        else:
            self.log.warn("No HoloClean session named " + name)
            return


class Session:
    """TODO. HoloClean Session Class"""

    def __init__(self, name, holo_env):
        logging.basicConfig()
        """TODO.


        Parameters
        ----------
        parameter : type
           This is a parameter

        Returns
        -------
        describe : type
            Explanation
        """

        # Initialize members
        self.name = name
        self.holo_env = holo_env
        self.dataset = None
        self.featurizers = []
        self.error_detectors = []
        self.cv = None

    # Internal methods
    def _numbskull_fg_lists(self):
        self.holo_env.logger.info('wrapper is starting')
        print "wrapper is starting"
        wrapper_obj = Wrapper(self.holo_env.dataengine, self.dataset)
        wrapper_obj.set_variable()
        wrapper_obj.set_weight()
        wrapper_obj.set_factor_to_var()
        wrapper_obj.set_factor()
        weight = wrapper_obj.get_list_weight()
        variable = wrapper_obj.get_list_variable()
        fmap = wrapper_obj.get_list_factor_to_var()
        factor = wrapper_obj.get_list_factor()
        edges = Wrapper.get_edge(factor)
        domain_mask = Wrapper.get_mask(variable)
        print "wrapper is finished"
        self.holo_env.logger.info('wrapper is finished')
        return weight, variable, factor, fmap, domain_mask, edges

    def _numskull(self):
        learn = 100
        self.holo_env.logger.info('numbskull is starting')
        print "numbskull is starting"
        ns = numbskull.NumbSkull(n_inference_epoch=100,
                                 n_learning_epoch=learn,
                                 quiet=True,
                                 learn_non_evidence=True,
                                 stepsize=0.0001,
                                 burn_in=100,
                                 decay=0.001 ** (1.0 / learn),
                                 regularization=1,
                                 reg_param=0.01)

        fg = self._numbskull_fg_lists()
        ns.loadFactorGraph(*fg)
        ns.learning()
        print "1"
        self.holo_env.logger.info('numbskull is finished')
        print "numbskull is finished"
        list_weight_value = []
        list_temp = ns.factorGraphs[0].weight_value[0]
        for i in range(0, len(list_temp)):
            list_weight_value.append([i, float(list_temp[i])])

        new_df_weights = self.holo_env.spark_session.createDataFrame(
            list_weight_value, ['weight_id', 'weight_val'])
        delete_table_query = 'drop table ' + \
            self.dataset.table_specific_name('Weights') + ";"
        self.holo_env.dataengine.query(delete_table_query)
        self.holo_env.dataengine.add_db_table(
            'Weights', new_df_weights, self.dataset)
        self.holo_env.logger.info('adding weight is finished')
        print "adding weight is finished is finished"

    # Setters
    def ingest_dataset(self, src_path):
        """TODO: Load, Ingest, and Analyze a dataset from a src_path"""
        self.holo_env.logger.info('ingesting file:' + src_path)
        self.dataset = Dataset()
        self.holo_env.dataengine.ingest_data(src_path, self.dataset)
        self.holo_env.logger.info(
            'creating dataset with id:' +
            self.dataset.print_id())
        return

    def add_featurizer(self, new_featurizer):
        """TODO: Add a new featurizer"""
        self.holo_env.logger.info('getting new signal for featurization...')
        self.featurizers.append(new_featurizer)
        self.holo_env.logger.info(
            'getting new signal for featurization is finished')
        return

    def add_error_detector(self, new_error_detector):
        """TODO: Add a new error detector"""
        self.holo_env.logger.info('getting the  for error detection...')
        self.error_detectors.append(new_error_detector)
        self.holo_env.logger.info('getting new for error detection')
        return

    def denial_constraints(self, filepath):
        self.Denial_constraints = []
        dc_file = open(filepath, 'r')
        for line in dc_file:
            self.Denial_constraints.append(line[:-1])

    # Getters

    def get_name(self):
        """TODO: Return session name"""
        return self.name

    def get_dataset(self):
        """TODO: Return session dataset"""
        return self.dataset

    # Methodsdata
    def ds_detect_errors(self):
        """TODO: Detect errors in dataset"""
        clean_cells = []
        dk_cells = []

        self.holo_env.logger.info('starting error detection...')
        for err_detector in self.error_detectors:
            temp = err_detector.get_noisy_dknow_dataframe(
                self.holo_env.dataengine.get_table_to_dataframe('Init', self.dataset))
            clean_cells.append(temp[1])
            dk_cells.append(temp[0])

        num_of_error_detectors = len(dk_cells)
        intersect_dk_cells = dk_cells[0]
        union_clean_cells = clean_cells[0]
        for detector_counter in range(1, num_of_error_detectors):
            intersect_dk_cells = intersect_dk_cells.intersect(
                dk_cells[detector_counter])
            union_clean_cells = union_clean_cells.unionAll(
                clean_cells[detector_counter])

        self.holo_env.dataengine.add_db_table(
            'C_clean', union_clean_cells, self.dataset)



        self.holo_env.dataengine.add_db_table(
            'C_dk', intersect_dk_cells, self.dataset)
        self.holo_env.logger.info('error detection is finished')

        return

    def ds_domain_pruning(self, pruning_threshold=0):
        self.holo_env.logger.info(
            'starting domain pruning with threshold %s',
            pruning_threshold)
        Pruning(
            self.holo_env.dataengine,
            self.dataset,
            self.holo_env.spark_session,
            pruning_threshold)
        self.holo_env.logger.info('Domain pruning is finished')
        return

    class FeatureWorker(Thread):
        def __init__(self, dataengine, query):
            self.dataengine = dataengine
            self.query = query

        def run(self):
            self.connection = self.dataengine._start_db()
            self.connection.execute(self.query)

    def parallel_queries(self, number_of_threads=multiprocessing.cpu_count() - 4, clean=1):
        print 'Creating parallel queries'
        t0 = time.time()
        list_of_names = []
        list_of_threads = []
        table_name = "clean" if clean == 1 else "dk"
        feature_name = "Feature_clean" if clean == 1 else "Feature_dk"
        t0 = time.time()

        for i in range(0, number_of_threads):
            list_of_threads.append(DatabaseWorker(table_name, self.list_of_queries, list_of_names,
                                                  self.holo_env, self.dataset, self.cv))

        t1 = time.time()
        print t1 - t0
        print 'Starting threads'
        for thread in list_of_threads:
            thread.start()

        for thread in list_of_threads:
            thread.join()

        insert_query = ""
        t1 = time.time()
        total = t1 - t0
        print "Total Featurization Queries time Before Union: "
        print total
        t0 = time.time()
        for name in list_of_names:
            insert_query = insert_query + " Select vid, assigned_val, feature, count from " + name + " UNION"
        insert_query = insert_query[:-5]

        insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name(
            feature_name) + " SELECT T_0.vid, T_0.assigned_val, T_0.feature, T_0.count FROM ( (" + insert_query + ")" \
                                                                  "as T_0);"
        print insert_query
        self.holo_env.dataengine.query(insert_signal_query)
        t1 = time.time()
        total = t1 - t0
        print "Union Time: "
        print total

        t0 = time.time()
        self._create_dimensions(clean)
        t1 = time.time()
        print 'Dimension time: ', t1 - t0
        return

    def ds_featurize(self, clean = 1):
        """TODO: Extract dataset features"""
        dc_query_prod = DCQueryProducer(clean, self.featurizers)
        dc_query_prod.start()
        num_of_threads = 4
        print 'Setting up Feature Threads'
        t0 = time.time()
        table_name = "Possible_values_clean" if clean == 1 else "Possible_values_dk"
        feature_name = "Feature_clean" if clean == 1 else "Feature_dk"

        query_for_featurization = "CREATE TABLE \
                                   " + self.dataset.table_specific_name(feature_name) \
                                  + "(vid INT, assigned_val INT," \
                                    " feature INT, count INT);"
        self.holo_env.dataengine.query(query_for_featurization)

        self.list_of_queries = []
        self.cv = Condition()

        feat_prod = FeatureProducer(clean, self.cv, self.list_of_queries, num_of_threads, self.featurizers)
        feat_prod.start()
        t1 = time.time()
        print t1 - t0
        self.parallel_queries(num_of_threads, clean)



    def _create_dimensions(self, clean = 1):
        dimensions = 'Dimensions_clean' if clean == 1 else 'Dimensions_dk'
        obs_possible_values = 'Observed_Possible_values_clean' if clean == 1 else 'Observed_Possible_values_dk'
        feature_id_map = 'Feature_id_map'
        kij_lookup = 'Kij_lookup_clean' if clean == 1 else 'Kij_lookup_dk'
        query_for_create_offset = "CREATE TABLE \
                    " + self.dataset.table_specific_name(dimensions) \
                                  + "(dimension Text, length INT);"
        self.holo_env.dataengine.query(query_for_create_offset)

        insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name(
            dimensions) + " SELECT 'N' as dimension, (" \
            " SELECT COUNT(*) FROM " \
            + self.dataset.table_specific_name(obs_possible_values) + ") as length;"
        self.holo_env.dataengine.query(insert_signal_query)
        insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name(
            dimensions) + " SELECT 'M' as dimension, (" \
            " SELECT COUNT(*) FROM " \
            + self.dataset.table_specific_name(feature_id_map) + ") as length;"

        self.holo_env.dataengine.query(insert_signal_query)
        insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name(
            dimensions) + " SELECT 'L' as dimension, MAX(m) as length FROM (" \
            " SELECT MAX(k_ij) m FROM " \
            + self.dataset.table_specific_name('Kij_lookup_clean') + " UNION " \
            " SELECT MAX(k_ij) as m FROM " \
            + self.dataset.table_specific_name('Kij_lookup_dk') + " ) k_ij_union;"
        self.holo_env.dataengine.query(insert_signal_query)


    def ds_learn_repair_model(self):
        """TODO: Learn a repair model"""
        return

    def ds_repair(self):
        """TODO: Returns suggested repair"""
        self.holo_env.logger.info('starting repairs')
        print "starting repairs"
        learning_obj = inference(
            self.holo_env.dataengine,
            self.dataset,
            self.holo_env.spark_session
        )
        learning_obj.learning()
        self.holo_env.logger.info('repairs are finished')
        print "repairs are finished"
        learning_obj.printing_results(self.holo_env.threshold, self.holo_env.first_k)
        return
