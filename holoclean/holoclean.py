#!/usr/bin/env python


import logging

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
import time

import torch
from dataengine import DataEngine
from dataset import Dataset
from featurization.database_worker import DatabaseWorker, QueryProducer
from utils.pruning import Pruning
from utils.parser_interface import ParserInterface
from threading import Condition, Semaphore
from collections import deque
import multiprocessing
from pyspark.sql.types import *

from errordetection.errordetector_wrapper import ErrorDetectorsWrapper
from featurization.initfeaturizer import SignalInit
from featurization.dcfeaturizer import SignalDC
from featurization.cooccurrencefeaturizer import SignalCooccur
from learning.softmax import SoftMax
from learning.accuracy import Accuracy


# Define arguments for HoloClean
arguments = [
    (('-path', '--holoclean_path'),
        {'metavar': 'HOLOCLEAN_PATH',
         'dest': 'holoclean_path',
         'default': '.',
         'type': str,
         'help': 'Path of HoloCLean'}),
    (('-u', '--db_user'),
        {'metavar': 'DB_USER',
         'dest': 'db_user',
         'default': 'holocleanuser',
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
    (('-pg', '--pg_driver'),
     {'metavar': 'PG_DRIVER',
      'dest': 'pg_driver',
      'default': 'holoclean/lib/postgresql-42.2.2.jar',
      'type': str,
      'help': 'Path for Postgresql PySpark driver'}),

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
      'help': 'The threshold of the probabilities '
              'which are shown in the results'}),
    (('-k', '--first_k'),
     {'metavar': 'FIRST_K',
      'dest': 'first_k',
      'default': 1,
      'type': int,
      'help': 'The final output will show the k-first results '
              '(if it is 0 it will show everything)'}),
    (('-l', '--learning-rate'),
     {'metavar': 'LEARNING_RATE',
      'dest': 'learning_rate',
      'default': 0.001,
      'type': float,
      'help': 'The learning rate holoclean will use during training'}),
    (('-p', '--pruning-threshold'),
     {'metavar': 'PRUNING_THRESHOLD',
      'dest': 'pruning_threshold',
      'default': 0.5,
      'type': float,
      'help': 'Threshold used for domain pruning step'}),
    (('-it', '--learning-iterations'),
     {'metavar': 'LEARNING_ITERATIONS',
      'dest': 'learning_iterations',
      'default': 20,
      'type': float,
      'help': 'Number of iterations used for softmax'}),
    (('-w', '--weight_decay'),
     {'metavar': 'WEIGHT_DECAY',
      'dest':  'weight_decay',
      'default': 0.9,
      'type': float,
      'help': 'The L2 penalty HoloClean will use during training'}),
    (('-p', '--momentum'),
     {'metavar': 'MOMENTUM',
      'dest': 'momentum',
      'default': 0.0,
      'type': float,
      'help': 'The momentum term in the loss function'}),
    (('-b', '--batch-size'),
     {'metavar': 'BATCH_SIZE',
      'dest': 'batch_size',
      'default': 1,
      'type': int,
      'help': 'The batch size during training'}),
    (('-t', '--timing-file'),
     {'metavar': 'TIMING_FILE',
      'dest': 'timing_file',
      'default': None,
      'type': str,
      'help': 'File to save timing infomrmation'})
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


class _Barrier:
    def __init__(self, n):
        self.n = n
        self.count = 0
        self.mutex = Semaphore(1)
        self.barrier = Condition()

    def wait(self):
        self.mutex.acquire()
        self.count = self.count + 1
        count = self.count
        self.mutex.release()
        if count == self.n:
            self.barrier.acquire()
            self.barrier.notifyAll()
            self.barrier.release()
        else:
            self.barrier.acquire()
            self.barrier.wait()
            self.barrier.release()


class HoloClean:
    """
    Main Entry Point for HoloClean.
    Creates a HoloClean Data Engine
    and initializes Spark.
    """

    def __init__(self, **kwargs):
        """
        Parameters
        ----------
        Parameters are set internally in module

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

        if self.verbose:
            logging.basicConfig(filename="logger.log",
                                filemode='w', level=logging.INFO)
        else:
            logging.basicConfig(filename="logger.log",
                                filemode='w', level=logging.ERROR)
        self.logger = logging.getLogger("__main__")
        # Initialize dataengine and spark session

        self.spark_session, self.spark_sql_ctxt = self._init_spark()
        self.dataengine = self._init_dataengine()

        # Init empty session collection
        self.session = {}
        self.session_id = 0

    # Internal methods
    def _init_dataengine(self):
        data_engine = DataEngine(self)
        return data_engine

    def _init_spark(self):
        # Set spark configuration
        conf = SparkConf()

        # Link MySQL driver to Spark Engine
        #conf.set("spark.executor.extraClassPath", self.mysql_driver)
        #conf.set("spark.driver.extraClassPath", self.mysql_driver)

        # Link PG driver to Spark
        conf.set("spark.executor.extraClassPath", self.holoclean_path + "/"+ self.pg_driver)
        conf.set("spark.driver.extraClassPath", self.holoclean_path + "/"+self.pg_driver)

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


class Session:
    """
    Session class controls the entire pipeline of HoloClean
    For Ingest call: ingest_dataset
    For DC Ingest: denial_constraints
    For Error Detection: add_error_detector, ds_detect_errors
    For Domain Prunning: ds_domain_pruning
    For Featurization: add_featurizer, ds_featurize
    """

    def __init__(self, holo_env, name="session"):
        logging.basicConfig()

        # Initialize members
        self.name = name
        self.holo_env = holo_env
        self.Denial_constraints = []
        self.featurizers = []
        self.error_detectors = []
        self.cv = None
        self.pruning = None
        self.dataset = Dataset()
        self.parser = ParserInterface(self)
        self.inferred_values = None

    def _timing_to_file(self, log):
        if self.holo_env.timing_file:
            t_file = open(self.holo_env.timing_file, 'a')
            t_file.write(log)

    def load_data(self, file_path):
        """ Loads a dataset from file into the database
        :param file_path: path to data file
        :return: pyspark dataframe
        """
        if self.holo_env.verbose:
            start = time.time()

        self._ingest_dataset(file_path)

        init = self.init_dataset

        if self.holo_env.verbose:
            end = time.time()
            log = 'Time to Load Data: ' + str(end - start) + '\n'
            print log
            if self.holo_env.timing_file:
                t_file = open(self.holo_env.timing_file, 'w')
                t_file.write(log)
        attributes = self.dataset.schema.split(',')
        for attribute in attributes:
            self.holo_env.dataengine.add_db_table_index(
                self.dataset.table_specific_name('Init'), attribute)
        return init

    def load_denial_constraints(self, file_path):
        """ Loads denial constraints from line-separated txt file
        :param file_path: path to dc file
        :return: string array of dc's
        """
        new_denial_constraints = self.parser.load_denial_constraints(
            file_path, self.Denial_constraints)
        self.Denial_constraints.extend(new_denial_constraints)
        return self.Denial_constraints

    def add_denial_constraint(self, dc):
        """ add denial constraints piecemeal from string into self.Denial_constraints
        :param dc: string in dc format
        :return: string array of dc's
        """
        checked_dc = self.parser.check_dc_format(dc, self.Denial_constraints)
        self.Denial_constraints.append(checked_dc)
        return self.Denial_constraints

    def remove_denial_constraint(self, index):
        """ removed the denial constraint at index
        :param index: index in list
        :return: string array of dc's
        """
        if len(self.Denial_constraints) == 0:
            raise IndexError("No Denial Constraints Have Been Defined")

        if index < 0 or index >= len(self.Denial_constraints):
            raise IndexError("Given Index Out Of Bounds")

        return self.Denial_constraints.pop(index)

    def load_clean_data(self, file_path):
        """
        Loads pre-defined clean cells from csv
        :param file_path: path to file
        :return: spark dataframe of clean cells
        """
        clean = self.holo_env.spark_session.read.csv(file_path, header=True)
        self.holo_env.dataengine.add_db_table('C_clean', clean, self.dataset)

        return clean

    def load_dirty_data(self, file_path):
        """
        Loads pre-defined dirty cells from csv
        :param file_path: path to file
        :return: spark dataframe of dirty cells
        """
        dirty = self.holo_env.spark_session.read.csv(file_path, header=True)
        self.holo_env.dataengine.add_db_table('C_dk', dirty, self.dataset)

        return dirty

    def detect_errors(self, detector):
        """ separates cells that violate DC's from those that don't

        :return: clean dataframe and don't know dataframe
        """
        if self.holo_env.verbose:
            start = time.time()

        err_detector = ErrorDetectorsWrapper(detector)

        self._add_error_detector(err_detector)
        self._ds_detect_errors()

        clean = self.clean_df
        dk = self.dk_df

        if self.holo_env.verbose:
            end = time.time()
            log = 'Time for Error Detection: ' + str(end - start) + '\n'
            print log
            self._timing_to_file(log)

        return clean, dk

    def repair(self):
        """
        repairs the initial data
        includes pruning, featurization, and softmax

        :return: repaired dataset
        """
        if self.holo_env.verbose:
            start = time.time()

        self._ds_domain_pruning(self.holo_env.pruning_threshold)

        if self.holo_env.verbose:
            end = time.time()
            log = 'Time for Domain Pruning: ' + str(end - start) + '\n'
            self.holo_env.logger.info("Total  pruning time:"+str(end - start))
            print log
            start = time.time()

        attr_constrained = self.parser.get_all_constraint_attributes(
            self.Denial_constraints)

        init_signal = SignalInit(attr_constrained,
                                 self.holo_env.dataengine,
                                 self.dataset)
        self._add_featurizer(init_signal)

        dc_signal = SignalDC(self.Denial_constraints, self)
        self._add_featurizer(dc_signal)

        self._ds_featurize(clean=1)

        if self.holo_env.verbose:
            end = time.time()
            log = 'Time for Featurization: ' + str(end - start) + '\n'
            print log
            self.holo_env.logger.info("Time for Featurization:" +
                                      str(end - start))

            start = time.time()

        soft = SoftMax(self, self.X_training)

        soft.logreg()

        if self.holo_env.verbose:
            end = time.time()
            log = 'Time for Training Model: ' + str(end - start) + '\n'
            print log
            self.holo_env.logger.info('Time for Training Model: ' +
                                      str(end - start))
            start = time.time()

        self._ds_featurize(clean=0)

        if self.holo_env.verbose:
            end = time.time()
            log = 'Time for Test Featurization: ' + str(end - start) + '\n'
            print log
            self.holo_env.logger.info('Time for Test Featurization dk: '
                                      + str(end - start))
            start = time.time()
        Y = soft.predict(soft.model, self.X_testing,
                         soft.setupMask(0, self.N, self.L))

        soft.save_prediction(Y)
        if self.holo_env.verbose:
            end = time.time()
            log = 'Time for Inference: ' + str(end - start) + '\n'
            print log
            self.holo_env.logger.info('Time for Inference: ' + str(end - start))

        self._create_corrected_dataset()

        return self.holo_env.dataengine.get_table_to_dataframe(
            'Repaired_dataset', self.dataset)

    def compare_to_truth(self, truth_path):
        """
        compares our repaired set to the truth
        prints precision and recall

        :param truth_path: path to clean version of dataset
        """

        flattening = 0

        acc = Accuracy(self, truth_path)
        acc.accuracy_calculation(flattening)

    # Setters
    def _ingest_dataset(self, src_path):
        """ Load, Ingest, a dataset from a src_path
        Tables created: Init
        Parameters
        ----------
        src_path : String
            string literal of path to the .csv file of the dataset

        Returns
        -------
        No Return
        """
        self.holo_env.logger.info('ingesting file:' + src_path)
        self.init_dataset = self.holo_env.dataengine.ingest_data(src_path, self.dataset)
        self.holo_env.logger.info(
            'creating dataset with id:' +
            self.dataset.print_id())
        return

    def _add_featurizer(self, new_featurizer):
        """Add a new featurizer
        Parameters
        ----------
        new_featurizer : Derived class of Featurizer
            Object representing one Feature Signal being used

        Returns
        -------
        No Return
        """
        if new_featurizer.id == 'SignalInit':
            maximum = self.holo_env.dataengine.query(
                "SELECT COALESCE(MAX(feature_ind), 0) as max FROM " +
                self.dataset.table_specific_name("Feature_id_map"), 1
            ).collect()[0]['max']
            index = maximum + 1
            list_domain_map = [[index, 'Init', 'Init', 'Init']]
            df_domain_map = self.holo_env.spark_session.createDataFrame(
                list_domain_map, StructType([
                    StructField("feature_ind", IntegerType(), True),
                    StructField("attribute", StringType(), False),
                    StructField("value", StringType(), False),
                    StructField("Type", StringType(), False),
                ]))
            self.holo_env.dataengine.add_db_table(
                'Feature_id_map', df_domain_map, self.dataset, append=1)
        elif new_featurizer.id == 'SignalCooccur':
            maximum = self.holo_env.dataengine.query(
                "SELECT COALESCE(MAX(feature_ind), 0) as max FROM " +
                self.dataset.table_specific_name("Feature_id_map"), 1
            ).collect()[0]['max']
            index = maximum + 1
            list_domain_map = []
            for attribute in self.pruning.domain_dict:
                value_index = 1
                for value in self.pruning.domain_dict[attribute]:
                    list_domain_map.append(
                        [index, attribute, unicode(value), 'cooccur'])
                    value_index += 1
                    index += 1
            df_domain_map = self.holo_env.spark_session.createDataFrame(
                list_domain_map, StructType([
                    StructField("feature_ind", IntegerType(), True),
                    StructField("attribute", StringType(), False),
                    StructField("value", StringType(), False),
                    StructField("Type", StringType(), False),
                ]))
            self.holo_env.dataengine.add_db_table(
                'Feature_id_map', df_domain_map, self.dataset, append=1)
        self.holo_env.logger.info('getting new signal for featurization...')
        self.featurizers.append(new_featurizer)
        self.holo_env.logger.info(
            'getting new signal for featurization is finished')
        return

    def _add_error_detector(self, new_error_detector):
        """Add a new error detector
        Parameters
        ----------
        :param new_error_detector : Derived class of ErrorDetectors
            Object representing a method of separating the dataset into
            a clean set and a dirty set

        Returns
        -------
        No Return
        """
        self.holo_env.logger.info('getting the  for error detection...')
        self.error_detectors.append(new_error_detector)
        self.holo_env.logger.info('getting new for error detection')
        return

    # Methods data
    def _ds_detect_errors(self):
        """
        Use added ErrorDetector to split the dataset into clean and dirty sets.
        Must be called after add_error_detector
        Tables Created: C_clean, C_dk
        :return: No return
        """
        clean_cells = []
        dk_cells = []

        self.holo_env.logger.info('starting error detection...')
        for err_detector in self.error_detectors:
            temp = err_detector.get_noisy_dknow_dataframe()
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

        self.clean_df = union_clean_cells

        self.holo_env.dataengine.add_db_table(
            'C_clean', union_clean_cells, self.dataset)

        self.holo_env.logger.info('The table: ' +
                                  self.dataset.table_specific_name('C_clean') +
                                  " has been created")
        self.holo_env.logger.info("  ")

        self.dk_df = intersect_dk_cells

        self.holo_env.dataengine.add_db_table(
            'C_dk', intersect_dk_cells, self.dataset)


        self.holo_env.logger.info('The table: ' +
                                  self.dataset.table_specific_name('C_dk') +
                                  " has been created")
        self.holo_env.logger.info("  ")
        self.holo_env.logger.info('error detection is finished')
        del union_clean_cells
        del intersect_dk_cells
        del self.error_detectors
        return

    def _ds_domain_pruning(self, pruning_threshold=0):
        """
        Prunes domain based off of threshold to give each cell
        repair candidates
        Tables created: Possible_values_clean, Possible_values_dk,
                        Observed_Possible_values_clean,
                        Observed_Possible_values_dk,
                        Kij_lookup_clean, Kij_lookup_dk, Feature_id_map_temp
        :param pruning_threshold: Float from 0.0 to 1.0
        :return: None
        """
        self.holo_env.logger.info(
            'starting domain pruning with threshold %s',
            pruning_threshold)

        self.pruning = Pruning(
            self,
            pruning_threshold)
        self.holo_env.logger.info('Domain pruning is finished :')
        return

    def _parallel_queries(self,
                          query_prod,
                          number_of_threads=multiprocessing.cpu_count(),
                          clean=1):
        list_of_threads = []

        cvX = Condition()
        if clean:
            b = _Barrier(number_of_threads + 1)
            for i in range(0, number_of_threads):
                list_of_threads.append(
                    DatabaseWorker(self, b, cvX))
            for thread in list_of_threads:
                thread.start()
            b.wait()
        else:
            b_dk = _Barrier(number_of_threads + 1)
            for i in range(0, number_of_threads):
                list_of_threads.append(
                    DatabaseWorker(self, b_dk, cvX))
            for thread in list_of_threads:
                thread.start()

            b_dk.wait()



        query_prod.join()
        if clean:
            self._create_dimensions(clean)
            X_training = torch.zeros(self.N, self.M, self.L)
            for thread in list_of_threads:
                thread.getX(X_training)
            cvX.acquire()
            cvX.notifyAll()
            cvX.release()
        else:
            self._create_dimensions(clean)
            X_testing = torch.zeros(self.N, self.M, self.L)
            for thread in list_of_threads:
                thread.getX(X_testing)
            cvX.acquire()
            cvX.notifyAll()
            cvX.release()

        for thread in list_of_threads:
            thread.join()

        if clean:
            self.X_training = X_training
            self.holo_env.logger.info("The X-Tensor_training has been created")
            self.holo_env.logger.info("  ")
        else:
            self.X_testing = X_testing
            self.holo_env.logger.info("The X-Tensor_testing has been created")
            self.holo_env.logger.info("  ")
        return

    def _ds_featurize(self, clean=1):
        """
        Extract dataset features and creates the X tensor for learning
        or for inferrence
        Tables created (clean=1): Dimensions_clean
        Tables created (clean=0): Dimensions_dk
        :param clean: Optional, default=1, if clean = 1 then
        the featurization is for clean cells otherwise dirty cells
        :return: None
        """

        self.holo_env.logger.info('Start executing queries for featurization')
        self.holo_env.logger.info(' ')
        num_of_threads = multiprocessing.cpu_count()
        query_prod = QueryProducer(self.featurizers, clean, num_of_threads)
        query_prod.start()

        self._parallel_queries(query_prod, num_of_threads, clean)

    def _create_dimensions(self, clean=1):
        dimensions = 'Dimensions_clean' if clean == 1 else 'Dimensions_dk'
        obs_possible_values = 'Observed_Possible_values_clean' if clean == 1 \
            else 'Observed_Possible_values_dk'
        feature_id_map = 'Feature_id_map'
        query_for_create_offset = "CREATE TABLE \
                    " + self.dataset.table_specific_name(dimensions) \
                                  + "(dimension VARCHAR(255), length INT);"
        self.holo_env.dataengine.query(query_for_create_offset)

        insert_signal_query = \
            "INSERT INTO " + self.dataset.table_specific_name(dimensions) + \
            " SELECT 'N' as dimension, (" \
            " SELECT COUNT(*) " \
            "FROM " + \
            self.dataset.table_specific_name(obs_possible_values) + \
            ") as length;"
        self.holo_env.dataengine.query(insert_signal_query)

        insert_signal_query = \
            "INSERT INTO " + self.dataset.table_specific_name(dimensions) + \
            " SELECT 'M' as dimension, (" \
            " SELECT COUNT(*) " \
            "FROM " \
            + self.dataset.table_specific_name(feature_id_map) + \
            ") as length;"
        self.holo_env.dataengine.query(insert_signal_query)

        insert_signal_query = \
            "INSERT INTO " + self.dataset.table_specific_name(dimensions) + \
            " SELECT 'L' as dimension, MAX(m) as length " \
            "FROM (" \
            " SELECT MAX(k_ij) m FROM " \
            + self.dataset.table_specific_name('Kij_lookup_clean') + \
            " UNION " \
            " SELECT MAX(k_ij) as m " \
            "FROM " \
            + self.dataset.table_specific_name('Kij_lookup_dk') + \
            " ) k_ij_union;"
        self.holo_env.dataengine.query(insert_signal_query)

        if (clean):
            dataframe_offset = \
                self.holo_env.dataengine.get_table_to_dataframe(
                    "Dimensions_clean", self.dataset)
            list = dataframe_offset.collect()
            dimension_dict = {}
            for dimension in list:
                dimension_dict[dimension['dimension']] = dimension['length']
            self.M = dimension_dict['M']
            self.N = dimension_dict['N']
            self.L = dimension_dict['L']

        else:
            dataframe_offset = self.holo_env.dataengine.get_table_to_dataframe(
                "Dimensions_dk", self.dataset)
            list = dataframe_offset.collect()
            dimension_dict = {}
            for dimension in list:
                dimension_dict[dimension['dimension']] = dimension['length']
            # X Tensor Dimensions (N * M * L)
            self.M = dimension_dict['M']
            self.N = dimension_dict['N']
            self.L = dimension_dict['L']
        return
    
    def reset_database(self):
        """
           we drop and re-create the database
        """
        query = "DROP DATABASE " + self.holo_env.db_name + ";"
        self.holo_env.dataengine.query(query)

        query = "CREATE DATABASE " + self.holo_env.db_name + ";"
        self.holo_env.dataengine.query(query)

    def _create_corrected_dataset(self):
        """
        Will recreate the original dataset with the repaired values and save
        to Repaired_dataset table in MySQL
        :return: the original dataset with the repaired values from the
        Inferred_values table
        """
        final = self.inferred_values
        init = self.init_dataset
        correct = init.collect()
        final = final.collect()
        for i in range(len(correct)):
            d = correct[i].asDict()
            correct[i] = Row(**d)
        for cell in final:
            d = correct[cell.tid - 1].asDict()
            d[cell.attr_name] = cell.attr_val
            correct[cell.tid - 1] = Row(**d)

        correct_dataframe = self.holo_env.spark_sql_ctxt.createDataFrame(correct)
        self.holo_env.dataengine.add_db_table("Repaired_dataset",
                                              correct_dataframe, self.dataset)
        return correct
