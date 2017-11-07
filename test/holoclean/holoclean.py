#!/usr/bin/env python

import sys
import logging
from dataengine import DataEngine
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from dataset import Dataset
from errordetection.errordetector import ErrorDetectors
from utils.pruning import Pruning
from featurization.featurizer import Featurizer
from learning.wrapper import Wrapper
from numbskull import NumbSkull 




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
         'default': 'holocleandb',
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
        dataEngine = DataEngine(self)
        return dataEngine

    def _init_spark(self):
        """TODO: Initialize Spark Session"""
        #if self.spark_session and self.spark_sql_ctxt:
         #   return

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
    def _wrapper(self):
	wrapper1=Wrapper(self.holo_env.dataengine,self.dataset)
	#wrapper1.set_variable()
	#wrapper1.set_weight()
	#wrapper1.set_Factor_to_Var()
	#wrapper1.set_factor()
	weight=wrapper1.spark_list_weight()
	variable=wrapper1.spark_list_variable()
	factor_to_var=wrapper1.spark_list_Factor_to_var()
	factor=wrapper1.spark_list_Factor()
	edge=wrapper1.create_edge(factor)
	domain_mask=wrapper1.create_mask(variable)
    	num=NumbSkull()
	num.loadFactorGraph(weight,variable,factor,factor_to_var,domain_mask,edge)

    # Setters
    def ingest_dataset(self, src_path):
        """TODO: Load, Ingest, and Analyze a dataset from a src_path"""
        self.dataset=Dataset()
        self.holo_env.dataengine.ingest_data(src_path,self.dataset)
	self.dataset.print_id()
        return

    def add_featurizer(self, newFeaturizer):
        """TODO: Add a new featurizer"""
        self.featurizers.append(newFeaturizer)
        return

    def add_error_detector(self, newErrorDetector):
        """TODO: Add a new error detector"""
        self.error_detectors.append(newErrorDetector)
        return

    def denial_constraints(self,filepath):
	self.Denial_constraints=[]
	dc_file=open(filepath,'r')
	for line in dc_file:
		self.Denial_constraints.append(line[:-1])
	print self.Denial_constraints
	

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

        for err_detector in self.error_detectors:
            temp = err_detector.get_noisy_dknow_dataframe(self.holo_env.dataengine._table_to_dataframe('Init', self.dataset))
            clean_cells.append(temp[1])
            dk_cells.append(temp[0])

        num_of_error_detectors = len(dk_cells)
        intersect_dk_cells = dk_cells[0]
        union_clean_cells = clean_cells[0]
        for detector_counter in range(1, num_of_error_detectors):
            intersect_dk_cells = intersect_dk_cells.intersect(dk_cells[detector_counter])
            union_clean_cells = union_clean_cells.unionAll(clean_cells[detector_counter])

        self.holo_env.dataengine.add_db_table('C_clean', union_clean_cells, self.dataset)
        self.holo_env.dataengine.add_db_table('C_dk', intersect_dk_cells, self.dataset)

        return

    def ds_domain_pruning(self):
	Pruning(self.holo_env.dataengine,self.dataset,self.holo_env.spark_session,0.5)
	return

    def ds_featurize(self):
        """TODO: Extract dataset features"""
	query_for_featurization='CREATE TABLE '+self.dataset.table_specific_name('Feature')+' AS (select * from ( '
	for feature in self.featurizers:
		query_for_featurization+=feature.get_query()+" union "
	query_for_featurization=query_for_featurization[:-7]
	query_for_featurization+=""")as Feature)order by rv_index,rv_attr,feature;ALTER TABLE """+self.dataset.table_specific_name('Feature')+""" MODIFY var_index INT AUTO_INCREMENT PRIMARY KEY;"""
	self.holo_env.dataengine.query(query_for_featurization)
	featurizer=Featurizer(self.Denial_constraints,self.holo_env.dataengine,self.dataset)
	featurizer.add_weights()
	
        return

    def ds_learn_repair_model(self):
        """TODO: Learn a repair model"""
        return

    def ds_repair(self):
        """TODO: Returns suggested repair"""
        return



