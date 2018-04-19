#!/usr/bin/env python

import logging

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time

import torch.nn.functional as F
import torch
from dataengine import DataEngine
from dataset import Dataset
from featurization.database_worker import DatabaseWorker, PopulateTensor
from utils.cooccurpruning import CooccurPruning
from utils.parser_interface import ParserInterface, DenialConstraint
import multiprocessing

from errordetection.errordetector_wrapper import ErrorDetectorsWrapper
from featurization.initfeaturizer import SignalInit
from featurization.dcfeaturizer import SignalDC
from featurization.cooccurrencefeaturizer import SignalCooccur
from global_variables import GlobalVariables
from learning.softmax import SoftMax
from learning.accuracy import Accuracy


# Defining arguments for HoloClean
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
    (('-pt1', '--pruning-threshold1'),
     {'metavar': 'PRUNING_THRESHOLD1',
      'dest': 'pruning_threshold1',
      'default': 0.0,
      'type': float,
      'help': 'Threshold1 used for domain pruning step'}),
    (('-pt2', '--pruning-threshold2'),
     {'metavar': 'PRUNING_THRESHOLD2',
      'dest': 'pruning_threshold2',
      'default': 0.1,
      'type': float,
      'help': 'Threshold2 used for domain pruning step'}),
    (('-pdb', '--pruning-dk-breakoff'),
     {'metavar': 'DK_BREAKOFF',
      'dest': 'pruning_dk_breakoff',
      'default': 5,
      'type': float,
      'help': 'DK breakoff used for domain pruning step'}),
    (('-pcb', '--pruning-clean-breakoff'),
     {'metavar': 'CLEAN_BREAKOFF',
      'dest': 'pruning_clean_breakoff',
      'default': 5,
      'type': float,
      'help': 'Clean breakoff used for domain pruning step'}),
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
    (('-ki', '--k-inferred'),
     {'metavar': 'K_INFERRED',
      'dest': 'k_inferred',
      'default': 1,
      'type': int,
      'help': 'Number of inferred values'}),
    (('-t', '--timing-file'),
     {'metavar': 'TIMING_FILE',
      'dest': 'timing_file',
      'default': None,
      'type': str,
      'help': 'File to save timing infomrmation'}),
]


# Flags for Holoclean mode
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
    """
    Main entry point for HoloClean which creates a HoloClean Data Engine
    and initializes Spark.
    """

    def __init__(self, **kwargs):
        """
        Constructor for Holoclean

        :param kwargs: arguments for HoloClean

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

    # Internal methods
    def _init_dataengine(self):
        """
        Creates dataengine object

        :return: dataengine
        """
        data_engine = DataEngine(self)
        return data_engine

    def _init_spark(self):
        """
        Set spark configuration

        :return: Spark session
        :return: Spark context
        """
        conf = SparkConf()

        # Link PG driver to Spark
        conf.set("spark.executor.extraClassPath",
                 self.holoclean_path + "/" + self.pg_driver)
        conf.set("spark.driver.extraClassPath",
                 self.holoclean_path + "/" + self.pg_driver)

        conf.set('spark.driver.memory', '20g')
        conf.set('spark.executor.memory', '20g')
        conf.set("spark.network.timeout", "6000")
        conf.set("spark.rpc.askTimeout", "99999")
        conf.set("spark.worker.timeout", "60000")
        conf.set("spark.driver.maxResultSize", '70g')
        conf.set("spark.ui.showConsoleProgress", "false")

        if self.spark_cluster:
            conf.set("spark.master", self.spark_cluster)

        # Gets Spark context
        sc = SparkContext(conf=conf)
        sc.setLogLevel("OFF")
        sql_ctxt = SQLContext(sc)
        return sql_ctxt.sparkSession, sql_ctxt


class Session:
    """
    Session class controls the entire pipeline of HoloClean
    """

    def __init__(self, holo_env, name="session"):
        """
        Constructor for Holoclean session
        :param holo_env: Holoclean object
        :param name: Name for the Holoclean session
        """
        logging.basicConfig()

        # Initialize members
        self.name = name
        self.holo_env = holo_env
        self.Denial_constraints = []  # Denial Constraint strings
        self.dc_objects = {}  # Denial Constraint Objects
        self.featurizers = []
        self.error_detectors = []
        self.cv = None
        self.pruning = None
        self.dataset = Dataset()
        self.parser = ParserInterface(self)
        self.inferred_values = None
        self.feature_count = 0

    def load_data(self, file_path):
        """
        Loads a dataset from file into the database

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
            print(log)
        attributes = self.dataset.get_schema('Init')
        for attribute in attributes:
            self.holo_env.dataengine.add_db_table_index(
                self.dataset.table_specific_name('Init'), attribute)
        return init

    def load_denial_constraints(self, file_path):
        """
        Loads denial constraints from line-separated txt file

        :param file_path: path to dc file

        :return: string array of dc's
        """
        new_denial_constraints, new_dc_objects = \
            self.parser.load_denial_constraints(
                file_path, self.Denial_constraints)
        self.Denial_constraints.extend(new_denial_constraints)
        self.dc_objects.update(new_dc_objects)
        return self.Denial_constraints

    def add_denial_constraint(self, dc):
        """
        Adds denial constraints from string into
        self.Denial_constraints

        :param dc: string in dc format

        :return: string array of dc's
        """
        dc_object = DenialConstraint(dc, self.dataset.get_schema("Init"))
        self.Denial_constraints.append(dc)
        self.dc_objects[dc] = dc_object
        return self.Denial_constraints

    def remove_denial_constraint(self, index):
        """
        Removing the denial constraint at index

        :param index: index in list

        :return: string array of dc's
        """
        if len(self.Denial_constraints) == 0:
            raise IndexError("No Denial Constraints Have Been Defined")

        if index < 0 or index >= len(self.Denial_constraints):
            raise IndexError("Given Index Out Of Bounds")

        self.dc_objects.pop(self.Denial_constraints[index])
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

    def create_blocks(self):
        max_number_block = 1
        blocks = {}
        objects = self.dc_objects
        for dc_name in self.dc_objects:
            group = {}
            for index in range(len(self.dc_objects[dc_name].predicates)):
                for lenght1 in range(len(
                        self.dc_objects[dc_name].predicates[index].components)):
                    if isinstance(self.dc_objects[dc_name].predicates[index].components[lenght1], list):
                        if self.dc_objects[dc_name].predicates[index].components[lenght1][1] not in blocks:
                            blocks[self.dc_objects[dc_name].predicates[index].components[lenght1][1]] = max_number_block
                        else:
                            group[
                                self.dc_objects[dc_name].predicates[
                                    index].components[lenght1][1]] = blocks[self.dc_objects[dc_name].predicates[index].components[lenght1][1]]
            if len(group)>0:
                pass


            max_number_block = max_number_block + 1



        print "mpika"

    def detect_errors(self, detector_list):
        """
        Separates cells that violate DC's from those that don't

        :param detector_list: List of error detectors

        :return: clean dataframe
        :return: don't know dataframe
        """
        #self.create_blocks()
        if self.holo_env.verbose:
            start = time.time()
        for detector in detector_list:
            err_detector = ErrorDetectorsWrapper(detector)
            self._add_error_detector(err_detector)

        self._ds_detect_errors()

        clean = self.clean_df
        dk = self.dk_df

        if self.holo_env.verbose:
            end = time.time()
            log = 'Time for Error Detection: ' + str(end - start) + '\n'
            print(log)

        return clean, dk

    def repair(self):
        """
        Repairs the initial data includes pruning, featurization, and softmax

        :return: repaired dataset
        """
        if self.holo_env.verbose:
            start = time.time()

        self._ds_domain_pruning(self.holo_env.pruning_threshold1,
                                self.holo_env.pruning_threshold2,
                                self.holo_env.pruning_dk_breakoff,
                                self.holo_env.pruning_clean_breakoff)

        if self.holo_env.verbose:
            end = time.time()
            log = 'Time for Domain Pruning: ' + str(end - start) + '\n'
            self.holo_env.logger.info("Total  pruning time:"+str(end - start))
            print log
            start = time.time()

        init_signal = SignalInit(self)
        self._add_featurizer(init_signal)

        dc_signal = SignalDC(self.Denial_constraints, self)
        self._add_featurizer(dc_signal)

        cooccur_signal = SignalCooccur(self)
        self._add_featurizer(cooccur_signal)

        # Trying to infer or to learn catch errors when tensors are none

        try:
            self._ds_featurize(clean=1)

            if self.holo_env.verbose:
                end = time.time()
                log = 'Time for Featurization: ' + str(end - start) + '\n'
                print log
                self.holo_env.logger.info("Time for Featurization:" +
                                          str(end - start))

                start = time.time()
            soft = SoftMax(self, self.X_training)
            soft.logreg(self.featurizers)

            if self.holo_env.verbose:
                end = time.time()
                log = 'Time for Training Model: ' + str(end - start) + '\n'
                print(log)
                self.holo_env.logger.info('Time for Training Model: ' +
                                          str(end - start))
                start = time.time()

        except Exception as e:
            self.holo_env.logger.\
                error('Error Creating Training Tensor: nothing to learn',
                      exc_info=e)

        try:

            self._ds_featurize(clean=0)
            if self.holo_env.verbose:
                end = time.time()
                log = 'Time for Test Featurization: ' + str(end - start) + '\n'
                print(log)
                self.holo_env.logger.\
                    info('Time for Test Featurization dk: ' + str(end - start))
                start = time.time()

            Y = soft.predict(soft.model, self.X_testing,
                             soft.setupMask(0, self.N, self.L))
            soft.save_prediction(Y)
            if self.holo_env.verbose:
                end = time.time()
                log = 'Time for Inference: ' + str(end - start) + '\n'
                print(log)
                self.holo_env.logger.info('Time for Inference: ' +
                                          str(end - start))

            soft.log_weights()

        except Exception as e:
            self.holo_env.logger.\
                error('Error Creating Prediction Tensor: nothing to infer',
                      exc_info=e)

        return self._create_corrected_dataset()

    def compare_to_truth(self, truth_path):
        """
        Compares our repaired set to the truth prints precision and recall

        :param truth_path: path to clean version of dataset
        """

        acc = Accuracy(self, truth_path)
        acc.accuracy_calculation()

    def _ingest_dataset(self, src_path):
        """
        Ingests a dataset from given source path

        :param src_path: string literal of path to the .csv file of the dataset

        :return: Null
        """
        self.holo_env.logger.info('ingesting file:' + src_path)
        self.init_dataset, self.attribute_map = \
            self.holo_env.dataengine.ingest_data(src_path, self.dataset)
        self.holo_env.logger.info(
            'creating dataset with id:' +
            self.dataset.print_id())
        all_attr = self.dataset.get_schema('Init')
        all_attr.remove(GlobalVariables.index_name)
        number_of_tuples = len(self.init_dataset.collect())
        tuples = [[i] for i in range(1, number_of_tuples + 1)]
        attr = [[a] for a in all_attr]

        tuples_dataframe = self.holo_env.spark_session.createDataFrame(
            tuples, ['ind'])
        attr_dataframe = self.holo_env.spark_session.createDataFrame(
            attr, ['attr'])
        self.init_flat = tuples_dataframe.crossJoin(attr_dataframe)
        return

    def _add_featurizer(self, new_featurizer):
        """
        Add a new featurizer

        :param new_featurizer:  Derived class of Featurizer Object
        representing one Feature Signal being used

        :return: Null
        """
        self.holo_env.logger.info('getting new signal for featurization...')
        self.featurizers.append(new_featurizer)
        self.holo_env.logger.info(
            'getting new signal for featurization is finished')
        return

    def _add_error_detector(self, new_error_detector):
        """
        Add a new error detector

        :param new_error_detector : Derived class of ErrorDetectors
            Object representing a method of separating the dataset into
            a clean set and a dirty set

        :return: Null

        """
        self.error_detectors.append(new_error_detector)
        self.holo_env.logger.info('Added new error detection')
        return

    # Methods data

    def _ds_detect_errors(self):
        """
        Using added error detectors to split the dataset into clean and
        dirty sets, must be called after add_error_detector

        :return: Null
        """
        clean_cells = []
        dk_cells = []

        self.holo_env.logger.info('starting error detection...')

        # Get clean and dk cells from each error detector
        for err_detector in self.error_detectors:
            temp = err_detector.get_noisy_dknow_dataframe()
            clean_cells.append(temp[1])
            dk_cells.append(temp[0])

        # Union all dk cells and intersect all clean cells
        num_of_error_detectors = len(dk_cells)
        union_dk_cells = dk_cells[0]
        intersect_clean_cells = clean_cells[0]
        for detector_counter in range(1, num_of_error_detectors):
            union_dk_cells = union_dk_cells.union(
                dk_cells[detector_counter])
            intersect_clean_cells = intersect_clean_cells.intersect(
                clean_cells[detector_counter])

        del dk_cells
        del clean_cells

        self.clean_df = intersect_clean_cells

        # Persist all clean and dk cells to Database
        self.holo_env.dataengine.add_db_table(
            'C_clean', intersect_clean_cells, self.dataset)

        self.holo_env.logger.info('The table: ' +
                                  self.dataset.table_specific_name('C_clean') +
                                  " has been created")
        self.holo_env.logger.info("  ")

        self.dk_df = union_dk_cells

        self.holo_env.dataengine.add_db_table(
            'C_dk', union_dk_cells, self.dataset)

        self.holo_env.logger.info('The table: ' +
                                  self.dataset.table_specific_name('C_dk') +
                                  " has been created")
        self.holo_env.logger.info("  ")
        self.holo_env.logger.info('error detection is finished')
        return

    def _ds_domain_pruning(self, pruning_threshold1, pruning_threshold2,
                           pruning_dk_breakoff, pruning_clean_breakoff):
        """
        Prunes domain based off of threshold 1 to give each cell
        repair candidates

        :param pruning_threshold1:
        Float variable to limit possible values for training data
        :param pruning_threshold2: Float variable to
        limit possible values for dirty data
        :param pruning_dk_breakoff: to limit possible values for dont
        know cells to less than k values
        :param pruning_clean_breakoff: to limit possible values for
        training data to less than k values

        :return: Null
        """
        self.holo_env.logger.info(
            'starting domain pruning with threshold %s',
            pruning_threshold1)

        self.pruning = CooccurPruning(
            self,
            pruning_threshold1, pruning_threshold2,
            pruning_dk_breakoff, pruning_clean_breakoff)
        self.pruning.get_domain()
        self.pruning._create_dataframe()

        self.holo_env.logger.info('Domain pruning is finished :')
        return

    def _parallel_queries(self,
                          number_of_threads=multiprocessing.cpu_count(),
                          clean=1):
        """
        Runs queries in parallel

        :param number_of_threads: the number of thread for parallel execution
        :param clean: flag to execute queries for clean or don't know data
        :return: Null
        """
        list_of_threads = []

        DatabaseWorker.table_names = []
        for i in range(0, number_of_threads):
            list_of_threads.append(DatabaseWorker(self))

        for thread in list_of_threads:
            thread.start()

        for thread in list_of_threads:
            thread.join()

        feature_tables = list(DatabaseWorker.table_names)
        self._create_dimensions(clean)

        try:

            X_tensor = torch.zeros(self.N, self.M, self.L)

            # Creates Threads to populate tensor
            tensor_threads = []
            for thread_num in range(len(list_of_threads)):
                thread = \
                    PopulateTensor(feature_tables[thread_num], X_tensor, self)
                tensor_threads.append(thread)
                thread.start()

            for feature in self.featurizers:
                if feature.direct_insert:
                    feature.insert_to_tensor(X_tensor, clean)

            # Waits for Threads that are populating tensor to be finished
            for thread in tensor_threads:
                thread.join()

            X_tensor = F.normalize(X_tensor, p=2, dim=1)

            if clean:
                self.X_training = X_tensor
                self.holo_env.logger.\
                    info("The X-Tensor_training has been created")
                self.holo_env.logger.info("  ")
            else:
                self.X_testing = X_tensor
                self.holo_env.logger.\
                    info("The X-Tensor_testing has been created")
                self.holo_env.logger.info("  ")

        except Exception:
            raise Exception("Error creating Tensor")
        return

    def _ds_featurize(self, clean=1):
        """
        Extracting dataset features and creates the X tensor for learning
        or for inferrence
        Tables created (clean=1): Dimensions_clean
        Tables created (clean=0): Dimensions_dk

        :param clean: Optional, default=1, if clean = 1 then
        the featurization is for clean cells otherwise dirty cells

        :return: Null
        """

        self.holo_env.logger.info('Start executing queries for featurization')
        self.holo_env.logger.info(' ')
        num_of_threads = multiprocessing.cpu_count()
        # Produce all queries needed to be run for featurization
        for featurizer in self.featurizers:
            queries_to_add = featurizer.get_query(clean)
            for query in queries_to_add:
                DatabaseWorker.queries.append(query)

        # Create multiple threads to execute all queries
        self._parallel_queries(num_of_threads, clean)

    def _create_dimensions(self, clean=1):
        """
        Finding the dimension for the tensor

        :param clean: Optional, default=1, if clean = 1 then
        the featurization is for clean cells otherwise dirty cells

        :return: Null
        """
        dimensions = 'Dimensions_clean' if clean == 1 else 'Dimensions_dk'
        possible_values = 'Possible_values_clean' if clean == 1 \
            else 'Possible_values_dk'
        feature_id_map = 'Feature_id_map'
        query_for_create_offset = "CREATE TABLE \
                    " + self.dataset.table_specific_name(dimensions) \
                                  + "(dimension VARCHAR(255), length INT);"
        self.holo_env.dataengine.query(query_for_create_offset)

        insert_signal_query = \
            "INSERT INTO " + self.dataset.table_specific_name(dimensions) + \
            " SELECT 'N' as dimension, (" \
            " SELECT MAX(vid) " \
            "FROM " + \
            self.dataset.table_specific_name(possible_values) + \
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

        if clean:
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
        Will re-create the original dataset with the repaired values
        and save it to Repaired_dataset table in Postgress
        Need to create the MAP first if we inferred more than 1 value

        :return: the original dataset with the repaired values from the
        Inferred_values table
        """

        if self.inferred_values:
            # Should not be empty has at least initial values
            # Persist it to compute MAP as max prediction
            self.holo_env.dataengine.add_db_table ("Inferred_values",
                                               self.inferred_values,
                                               self.dataset)
        else:
            self.holo_env.logger.info(" No Inferred_values ")
            self.holo_env.logger.info("  ")
            return

        init = self.init_dataset.collect()
        attribute_map = {}
        index = 0
        for attribute in self.dataset.attributes['Init']:
            attribute_map[attribute] = index
            index += 1
        corrected_dataset = []

        for i in range(len(init)):
            row = []
            for attribute in self.dataset.attributes['Init']:
                row.append(init[i][attribute])
            corrected_dataset.append(row)

        # Compute MAP if k_inferred > 1
        if self.holo_env.k_inferred > 1:
            map_inferred_query = "SELECT  I.* FROM " + \
                                 self.dataset.table_specific_name(
                                     'Inferred_Values') + " AS I , (SELECT " \
                                                           "tid, attr_name, " \
                                                           "MAX(probability) " \
                                                           "AS max_prob FROM " + \
                                 self.dataset.table_specific_name(
                                     'Inferred_Values') + "  GROUP BY tid, " \
                                                          "attr_name) AS M " \
                                                          "WHERE I.tid = " \
                                                          "M.tid AND " \
                                                          "I.attr_name = " \
                                                          "M.attr_name AND " \
                                                          "I.probability = " \
                                                          "M.max_prob"

            self.inferred_map = self.holo_env.dataengine.query(
                map_inferred_query, 1)
        else:
            # MAP is the inferred values when inferred k = 1
            self.inferred_map = self.inferred_values

        # persist another copy for now
        # needed for accuracy calculations
        self.holo_env.dataengine.add_db_table("Inferred_map",
                                              self.inferred_map,
                                              self.dataset)

        # the corrections
        value_predictions = self.inferred_map.collect()

        # Replace values with the inferred values from multi value predictions
        if value_predictions:
            for j in range(len(value_predictions)):
                tid = value_predictions[j]['tid'] - 1
                column = attribute_map[value_predictions[j]['attr_name']]
                corrected_dataset[tid][column] =\
                    value_predictions[j]['attr_val']

        correct_dataframe = \
            self.holo_env.spark_sql_ctxt.createDataFrame(
                corrected_dataset, self.dataset.attributes['Init'])

        self.holo_env.dataengine.add_db_table("Repaired_dataset",
                                              correct_dataframe, self.dataset)

        self.holo_env.logger.info("The Inferred_values, MAP,  "
                                  "and Repaired tables have been created")
        self.holo_env.logger.info("  ")

        return correct_dataframe
