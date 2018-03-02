#!/usr/bin/env python


import logging

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
import time

import torch
from dataengine import DataEngine
from dataset import Dataset
from featurization.DatabaseWorker import DatabaseWorker, FeatureProducer, \
    DCQueryProducer
from utils.pruning import Pruning
from threading import Condition, Semaphore
import threading
from collections import deque
import multiprocessing

from errordetection.errordetector import ErrorDetectors
from featurization.featurizer import SignalInit, SignalCooccur, SignalDC
from learning.softmax import SoftMax
from learning.accuracy import Accuracy

from DCFormatException import DCFormatException

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
      'help': 'The threshold of the probabilities '
              'which are shown in the results'}),
    (('-k', '--first_k'),
     {'metavar': 'FIRST_K',
      'dest': 'first_k',
      'default': 1,
      'type': int,
      'help': 'The final output will show the k-first results '
              '(if it is 0 it will show everything)'}),
     (('-l','--learning-rate'),
      {'metavar':'LEARNING_RATE',
       'dest':'learning_rate',
       'default': 0.001,
       'type': float,
       'help': 'The learning rate holoclean will use during training'}),
     (('-w', '--weight_decay'),
      {'metavar':'WEIGHT_DECAY',
       'dest':  'weight_decay',
       'default': 0.9,
       'type': float,
       'help': 'The weight decay HoloClean will use during training, this is an L2 penalty'}),
     (('-p', '--momentum'),
      {'metavar':'MOMENTUM',
       'dest': 'momentum',
       'default': 0.0,
       'type': float,
       'help': 'The momentum term in the loss function'}),
    (('-b', '--batch-size'),
     {'metavar': 'BATCH_SIZE',
      'dest': 'batch_size',
      'default':1,
      'type': int,
      'help': 'The batch size during training'})
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
        string_name = str(threading.currentThread().getName())
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
        self.dataset = None
        self.Denial_constraints = []
        self.featurizers = []
        self.error_detectors = []
        self.cv = None

    def load_data(self, file_path):
        """ Loads a dataset from file into the database
        :param file_path: path to data file
        :return: pyspark dataframe
        """
        self._ingest_dataset(file_path)

        init = self.holo_env.dataengine.get_table_to_dataframe(
            'Init', self.dataset)

        return init

    def load_denial_constraints(self, file_path):
        """ Loads denial constraints from line-separated txt file
        :param file_path: path to dc file
        :return: string array of dc's
        """
        self._denial_constraints(file_path)
        return self.Denial_constraints

    def add_denial_constraint(self, dc):
        """ add denial constraints piecemeal from string
        :param dc: string in dc format
        :return: string array of dc's
        """
        self._check_dc_format(dc)

        self.Denial_constraints.append(dc)
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

    def _check_dc_format(self, dc):
        """
        determines whether or not the dc is formatted correctly
        used before adding any dc to the list

        :param dc: denial constraint to be checked

        :return nothing if dc is correctly formatted
        raises exception if incorrect
        """

        if dc in self.Denial_constraints:
            raise DCFormatException("Duplicate Denial Constraint")

        split_dc = dc.split('&')

        if len(split_dc) < 3:
            raise DCFormatException("Invalid DC: Missing Information")

        if split_dc[0] != 't1' or split_dc[1] != 't2':
            raise DCFormatException("Invalid DC: "
                                    "Tuples Not Defined Correctly")

        operators = ['EQ', 'LT', 'GT', 'IQ', 'LTE', 'GTE']

        for inequality in split_dc[2:]:
            split_ie = inequality.split('(')

            if len(split_ie) != 2:
                raise DCFormatException("Invalid DC: "
                                        "Inequality Not Defined Correctly")

            if split_ie[0] == '':
                raise DCFormatException("Invalid DC: "
                                        "Missing Operator")

            if split_ie[0] not in operators:
                raise DCFormatException("Invalid DC: "
                                        "Operator Must Be In " +
                                        str(operators))

            split_tuple = split_ie[1].split(',')
            if len(split_tuple) != 2:
                raise DCFormatException("Invalid DC: "
                                        "Tuple Not Defined Correctly")

    def detect_errors(self):
        """ separates cells that violate DC's from those that don't

        :return: clean dataframe and don't know dataframe
        """
        err_detector = ErrorDetectors(self.Denial_constraints,
                                      self.holo_env,
                                      self.dataset)

        self._add_error_detector(err_detector)
        self._ds_detect_errors()

        clean = self.holo_env.dataengine.get_table_to_dataframe(
            'C_clean', self.dataset)
        dk = self.holo_env.dataengine.get_table_to_dataframe(
            'C_dk', self.dataset)

        return clean, dk

    def repair(self):
        """
        repairs the initial data
        includes pruning, featurization, and softmax

        :return: repaired dataset
        """
        self._ds_domain_pruning(0.5)

        init_signal = SignalInit(self.Denial_constraints,
                                 self.holo_env.dataengine,
                                 self.dataset)
        self._add_featurizer(init_signal)

        cooccur_signal = SignalCooccur(self.Denial_constraints,
                                       self.holo_env.dataengine,
                                       self.dataset)
        self._add_featurizer(cooccur_signal)

        dc_signal = SignalDC(self.Denial_constraints,
                             self.holo_env.dataengine,
                             self.dataset,
                             self.holo_env.spark_session)
        self._add_featurizer(dc_signal)

        self._ds_featurize(clean=1)

        soft = SoftMax(self.holo_env.dataengine, self.dataset,
                       self.holo_env, self.X_training)

        soft.logreg()

        self._ds_featurize(clean=0)

        Y = soft.predict(soft.model, self.X_testing,
                         soft.setupMask(0, self.N, self.L))
        soft.save_prediction(Y)

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

        acc = Accuracy(self.holo_env.dataengine, truth_path, self.dataset,
                       self.holo_env.spark_session)
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
        self.dataset = Dataset()
        self.holo_env.dataengine.ingest_data(src_path, self.dataset)
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

    def _denial_constraints(self, filepath):
        """
        Read a textfile containing the the Denial Constraints
        :param filepath: The path to the file containing DCs
        :return: None
        """
        dc_file = open(filepath, 'r')
        for line in dc_file:
            if line.translate(None, ' \n') != '':
                self._check_dc_format(line[:-1])
                self.Denial_constraints.append(line[:-1])

    # Methodsdata
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
            temp = err_detector.get_noisy_dknow_dataframe(
                self.holo_env.dataengine.get_table_to_dataframe(
                    'Init', self.dataset))
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

        self.holo_env.logger.info('The table: ' +
                                  self.dataset.table_specific_name('C_clean') +
                                  " has been created")
        self.holo_env.logger.info("  ")

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
        Pruning(
            self.holo_env.dataengine,
            self.dataset,
            self.holo_env.spark_session,
            pruning_threshold)
        self.holo_env.logger.info('Domain pruning is finished')
        return

    def _parallel_queries(self,
                          dc_query_prod,
                          number_of_threads=multiprocessing.cpu_count() - 2,
                          clean=1):
        list_of_names = []
        list_of_threads = []
        table_name = "clean" if clean == 1 else "dk"

        if clean:
            b = _Barrier(number_of_threads + 1)
            for i in range(0, number_of_threads):
                list_of_threads.append(
                    DatabaseWorker(table_name, self.list_of_queries,
                                   list_of_names, self.holo_env,
                                   self.dataset, self.cv, b, self.cvX))
            for thread in list_of_threads:
                thread.start()

            b.wait()
        else:
            b1 = _Barrier(number_of_threads + 1)
            for i in range(0, number_of_threads):
                list_of_threads.append(
                    DatabaseWorker(table_name, self.list_of_queries,
                                   list_of_names, self.holo_env,
                                   self.dataset, self.cv, b1, self.cvX))
            for thread in list_of_threads:
                thread.start()

            b1.wait()

        dc_query_prod.join()
        if (clean):
            self._create_dimensions(clean)
            X_training = torch.zeros(self.N, self.M, self.L)
            for thread in list_of_threads:
                thread.getX(X_training)
            self.cvX.acquire()
            self.cvX.notifyAll()
            self.cvX.release()
        else:
            self._create_dimensions(clean)
            X_testing = torch.zeros(self.N, self.M, self.L)
            for thread in list_of_threads:
                thread.getX(X_testing)
            self.cvX.acquire()
            self.cvX.notifyAll()
            self.cvX.release()

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
        dc_query_prod = DCQueryProducer(clean, self.featurizers)
        dc_query_prod.start()
        num_of_threads = multiprocessing.cpu_count() - 2

        self.list_of_queries = deque([])
        self.cv = Condition()
        self.cvX = Condition()

        feat_prod = FeatureProducer(clean, self.cv, self.list_of_queries,
                                    num_of_threads, self.featurizers)
        feat_prod.start()
        t1 = time.time()
        self._parallel_queries(dc_query_prod, num_of_threads, clean)

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

    def _create_corrected_dataset(self):
        """
        Will recreate the original dataset with the repaired values and save
        to Repaired_dataset table in MySQL
        :return: the original dataset with the repaired values from the
        Inferred_values table
        """
        final = self.holo_env.dataengine.get_table_to_dataframe(
            "Inferred_values", self.dataset).select(
            "tid", "attr_name", "attr_val")
        init = self.holo_env.dataengine.get_table_to_dataframe(
            "Init", self.dataset)
        correct = init.collect()
        final = final.collect()
        for i in range(len(correct)):
            d = correct[i].asDict()
            correct[i] = Row(**d)
        for cell in final:
            d = correct[cell.tid - 1].asDict()
            d[cell.attr_name] = cell.attr_val
            correct[cell.tid - 1] = Row(**d)
            
        correct_dataframe = self.holo_env.spark_sql_ctxt.createDataFrame(
            correct)
        self.holo_env.dataengine.add_db_table("Repaired_dataset",
                                              correct_dataframe, self.dataset)
        return correct
