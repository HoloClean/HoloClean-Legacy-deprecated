#!/usr/bin/env python


import logging
import sys

import numbskull
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from dataengine import DataEngine
from dataset import Dataset
from featurization.featurizer import Featurizer
from learning.inference import inference
from learning.accuracy import Accuracy
from learning.wrapper import Wrapper
from learning.accu import  Accu
from preprocessing.preprocessing import Preprocessing

# Define arguments for Holofusion
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
         'default': 'lib/mysql-connector-java-5.1.44-bin.jar',
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
    (('-mj', '--majority_vote'),
     {'metavar': 'MAJORITY_VOTE',
      'dest': 'majority_vote',
      'default': 1,
      'type': int,
      'help': 'If it is 1 all the weights are one. If it is 0 we use numbskull to learn them'}),
    (('-tr', '--training_data'),
     {'metavar': 'TRAINING DATA',
      'dest': 'training_data',
      'default': 0,
      'type': int,
      'help': 'If it is 1 we have a ground truth that we will use as training data'}),
    (('-si', '--multiple_weight'),
     {'metavar': 'MULTIPLE WEIGHT',
      'dest': 'mutliple_weight',
      'default': 0,
      'type': int,
      'help': 'If it is 1 we have multiple weight for each source id'}),

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


class HoloFusion:
    """TODO.
    Main Entry Point for HoloFusion.
    Creates a HoloFusion Data Engine
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

        self.key = "date_symbol"
        self.attribute_to_check = "percent_change"
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
                            filemode='w', level=logging.INFO)
        self.logger = logging.getLogger("__main__")
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
        # Initialize dataengine and spark session

        self.spark_session, self.spark_sql_ctxt = self._init_spark()
        self.dataengine = self._init_dataengine()

        # Init empty session collection
        self.session = {}
        self.session_id = 0

    # Internal methods
    def _init_dataengine(self):
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
        sc.setLogLevel("ERROR")
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
        newSession = HoloFusionSession(name + str(self.session_id))
        self.section_id += 1
        return newSession

    def get_session(self, name):
        if name in self.session:
            return self.session[name]
        else:
            self.log.warn("No HoloClean session named " + name)
            return


class HoloFusionSession:

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
        if self.holo_env.training_data:
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

        else:
            ns = numbskull.NumbSkull(n_inference_epoch=0,
                                     n_learning_epoch=1000,
                                     stepsize=0.1,
                                     decay=1.0,
                                     reg_param=0.1,
                                     regularization=2,
                                     quiet=True,
                                     truncation=10,
                                     verbose=False,
                                     learn_non_evidence=True,
                                     burn_in=5,
                                     nthreads=1)

            fg = self._numbskull_fg_lists()
            ns.loadFactorGraph(*fg)
            ns.learning()
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
        """TODO:        self.holo_env.logger.info('ingesting file:' + src_path)
        self.dataset = Dataset()
        self.hLoad, Ingest, and Analyze a dataset from a src_path"""
        self.holo_env.logger.info('ingesting file:' + src_path)
        self.dataset = Dataset()
        self.holo_env.dataengine.ingest_data(src_path, self.dataset)
        self.holo_env.logger.info(
            'creating dataset with id:' +
            self.dataset.print_id())
        return

    def adding_training_data(self,src_path):
        """TODO:        self.holo_env.logger.info('ingesting file:' + src_path)
         self.dataset = Dataset()
         self.hLoad, Ingest, and Analyze a dataset from a src_path"""

        preprocessing = Preprocessing(self.holo_env.spark_session, self.holo_env.dataengine, self.dataset, src_path)
        if self.holo_env.training_data:
            preprocessing.adding_training_data()
            preprocessing.creating_c_clean_table()
            preprocessing.creating_c_dk()
        else:
            preprocessing.creating_tables()

        return


    # Getters
    def get_name(self):
        """TODO: Return session name"""
        return self.name
    def get_dataset(self):
        """TODO: Return session dataset"""
        return self.dataset

    # Methodsdata
    def feature(self):
        featurizer = Featurizer(self.holo_env.dataengine, self.dataset)
        if self.holo_env.majority_vote != 2:
            featurizer.create_feature()
            print ('adding weight_id to feature table...')
            self.holo_env.logger.info('adding weight_id to feature table...')
            featurizer.add_weights()
            self.holo_env.logger.info(
            'adding weight_id to feature table is finished')
            print (
            'adding weight_id to feature table is finished')
        else:
           self.src_observations = featurizer.create_dictionaries()
        return

    def wrapper(self):
        self._numskull()

    def inference(self):
        infe = inference(self.holo_env.dataengine, self.dataset, self.holo_env.spark_session)
        if self.holo_env.majority_vote == 1:
            infe.majority_vote()
            infe.set_probabilities()
        elif self.holo_env.majority_vote == 0:
            self._numskull()
            infe.set_probabilities()
        else:
         labelled={}
         accu=Accu(labelled, self.src_observations,self.holo_env.dataengine, self.dataset, self.holo_env.spark_session)
         accu.solve(iterations=100)


        #infe.add_truth()
        return

    def accuracy(self, path_to_ground_truth):
        accuracy = Accuracy(self.holo_env.dataengine, path_to_ground_truth, self.dataset, self.holo_env.spark_session)
        accuracy.create_correct_table()
        accuracy.fusion_accuracy()
        return
