#!/usr/bin/env python

import sys
import logging
from holoclean.dataengine import DataEngine
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

# Define arguments for HoloClean
arguments = [
    (('-u', '--db_user'),
        {'metavar': 'DB_USER',
         'dest': 'db_user',
         'default': 'holoclean',
         'type': str,
         'help': 'User for DB used to persist state'}),
    (('-p', '--password', '--pass'),
        {'metavar': 'PASSWORD',
         'dest': 'db_pwd',
         'default': '',
         'type': str,
         'help': 'Password for DB used to persist state'}),
    (('-h', '--host'),
        {'metavar': 'HOST',
         'dest': 'db_host',
         'default': '127.0.0.1',
         'type': str,
         'help': 'Host for DB used to persist state'}),
    (('-d', '--database'),
        {'metavar': 'DATABASE',
         'dest': 'db_name',
         'default': 'holostate',
         'type': str,
         'help': 'Name of DB used to persist state'}),
    (('-m', '--mysql_driver'),
        {'metavar': 'MYSQL_DRIVER',
         'dest': 'mysql_driver',
         'default': None,
         'type': str,
         'help': 'Path for MySQL driver'}),
    (('-s', '--spark_cluster'),
        {'metavar': 'SPARK',
         'dest': 'spark_cluster',
         'default': None,
         'type': str,
         'help': 'Spark cluster address'}),
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

        # Initialize additional arguments
        for (arg, default) in arg_defaults.items():
            setattr(self, arg, kwargs.get(arg, default))

        # Set verbose and initialize logging
        if self.verbose:
            self.log = logging.basicConfig(stream=sys.stdout,
                                           level=logging.DEBUG)
        else:
            self.log = None

        # Initialize dataengine and spark session
        self.dataengine = self._init_dataengine()
        self.spark_session, self.spark_sql_ctxt = self._init_spark()

        # Init empty session collection
        self.session = {}
        self.session_id = 0

    # Internal methods
    def _init_dataengine(self):
        """TODO: Initialize HoloClean's Data Engine"""
        if self.dataengine:
            return
        dataEngine = DataEngine(self)
        return dataEngine

    def _init_spark(self):
        """TODO: Initialize Spark Session"""
        if self.spark_session and self.spark_sql_ctxt:
            return

        # Set spark configuration
        conf = SparkConf()
        # Link MySQL driver to Spark Engine
        conf.set("spark.executor.extraClassPath", self.mysql_driver)
        conf.set("spark.driver.extraClassPath", self.mysql_driver)
        if self.spark_cluster:
            conf.set("spark.master", self.spark_cluster)

        # Get Spark context
        sc = SparkContext(conf=conf)
        sql_ctxt = SQLContext(sc)
        return sql_ctxt.sparkSession, sql_ctxt

    # Setters
    def set_dataengine(self, newDataEngine):
        """TODO: Manually set Data Engine"""
        self.dataengine = newDataEngine
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
        newSession = Session(name+str(self.session_id))
        self.section_id += 1
        return newSession

    def get_session(self, name):
        if name in self.session:
            return self.session[name]
        else:
            self.log.warn("No HoloClean session named "+name)
            return


class Session:
    """TODO. HoloClean Session Class"""

    def __init__(self, name, holo_env):
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

    # Setters
    def ingest_dataset(self, src_path):
        """TODO: Load, Ingest, and Analyze a dataset from a src_path"""
        return

    def add_featurizer(self, newFeaturizer):
        """TODO: Add a new featurizer"""
        self.featurizers.append(newFeaturizer)
        return

    def add_error_detector(self, newErrorDetector):
        """TODO: Add a new error detector"""
        self.error_detectors.append(newErrorDetector)
        return

    # Getters
    def get_name(self):
        """TODO: Return session name"""
        return self.name

    def get_dataset(self):
        """TODO: Return session dataset"""
        return self.dataset

    # Methods
    def ds_detect_errors(self):
        """TODO: Detect errors in dataset"""
        return

    def ds_featurize(self):
        """TODO: Extract dataset features"""
        return

    def ds_learn_repair_model(self):
        """TODO: Learn a repair model"""
        return

    def ds_repair(self):
        """TODO: Returns suggested repair"""
        return

    # def _error_detection(self,denial_constarint_standard_list,data_dataframe = None):
    #     """
    #     This method will fill the two table clean and dont-know cell in the dataset that assigned to the dataegine
    #
    #     """
    #     if data_dataframe is None:
    #         data_dataframe = self.dataengine.get_table_spark('T')
    #
    #     self.err=errordetector.ErrorDetectors(denial_constarint_standard_list,self.dataengine,self.spark)
    #     dk_cells_dataframes,clean_cells_dataframes=self.err.fill_table(data_dataframe)
    #     self.dataengine.register_spark('C_clean',clean_cells_dataframes)
    #     self.dataengine.register_spark('C_dk',dk_cells_dataframes)
    #
    #     return dk_cells_dataframes,clean_cells_dataframes
    #
    # def _domain_prunnig(self,data_dataframe = None,c_dk_dataframe = None,new_threshold = None):
    #     """
    #     This method will change fill the table D of the dataset that assigned to the dataengine
    #
    #     """
    #
    #     if data_dataframe is None:
    #         data_dataframe = self.dataengine.get_table_spark('T')
    #     if c_dk_dataframe is None:
    #         c_dk_dataframe = self.dataengine.get_table_spark('C_dk')
    #
    #     dom_prun=domainpruning.DomainPruning(data_dataframe,c_dk_dataframe)
    #
    #     if new_threshold is not None:
    #         dom_prun.set_threshold(new_threshold)
    #
    #     p_domain_df = dom_prun.allowable_doamin_value(self.spark)
    #
    #     self.dataengine.register_spark('D',p_domain_df)
    #
    #     return p_domain_df
    #
    #
    # def _featurizer(self,denial_constraints,data_dataframe = None,):
    #     """
    #     This method will fill the X based on the 3 signal that we have in the project
    #
    #     """
    #     if data_dataframe is None:
    #         data_dataframe = self.dataengine.get_table_spark('T')
    #     dc_featurizer=dcfeaturizer.DCFeaturizer(data_dataframe,denial_constraints)
    #     dc_features=dc_featurizer.featurize()