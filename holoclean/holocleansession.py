import sys
import pyspark as ps
import dataengine as de
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, Row
sys.path.append('../../')
from holoclean.errordetection import errordetector
from holoclean.utils import domainpruning




class HolocleanSession:
    

    def __init__(self,driver_path,spark_cluster_path = None):
        if spark_cluster_path is not None:
            self.spark_cluster_path=spark_cluster_path
        self._start_spark_session(driver_path,spark_cluster_path)
    def set_dataengine(self,dataengine):
        self.dataengine=dataengine
    def _covert2_spark_dataframe(self,table_name):
        
        """
        This function by getting the name of the table load it to Spark dataframe
        """
        panda_dataframe=  self.dataengine.get_table(table_name)
        col_names=self.dataengine.get_schema(table_name).split(',')
        spark_df = self.spark_session.createDataFrame(panda_dataframe,col_names)
        
        return spark_df
    
    def _start_spark_session(self,filepath,spark_cluster_path = None):

    	conf=SparkConf()
    	conf.set("spark.executor.extraClassPath", filepath)
    	conf.set("spark.driver.extraClassPath", filepath)
        if spark_cluster_path is not None:
            #spark_cluster_path would be something like 127.0.0.1:7707           
            conf.set("spark.master", spark_cluster_path)
             
          #  if spark_cluster_path is None:
                #   spark_cluster_path would be something like 127.0.0.1:7707           
           #      self.spark_session=ps.sql.SparkSession.builder.master("local").appName("Holoclean Session").getOrCreate()
           # else:
            #    self.spark_session=ps.sql.SparkSession.builder.master(self.spark_cluster_path).appName("Holoclean Session").getOrCreate()
        sc = SparkContext(conf=conf)
        self.sql = SQLContext(sc)
	
        self.spark=self.sql.sparkSession

    def returnspark_session(self):
        return self.spark

    def return_sqlcontext(self):
        return self.sql
           

    def _error_detection(self,denial_constarint_standard_list,data_dataframe = None):
        """
        This method will fill the two table clean and dont-know cell in the dataset that assigned to the dataegine
        
        """
        if data_dataframe is None:
            data_dataframe = self.dataengine.get_table_spark('T')
        
        err=errordetector.ErrorDetectors(denial_constarint_standard_list,self.dataengine,self.spark)
        dk_cells_dataframes,clean_cells_dataframes=err.fill_table(data_dataframe)
        self.dataengine.register_spark('C_clean',clean_cells_dataframes)
        self.dataengine.register_spark('C_dk',dk_cells_dataframes)
        
        return dk_cells_dataframes,clean_cells_dataframes
        
    def _domain_prunnig(self,c_dk_dataframe = None,data_dataframe = None,new_threshold = None):
        """
        This method will change fill the table D of the dataset that assigned to the dataengine
        
        """
        if c_dk_dataframe is None:
            c_dk_dataframe = self.dataengine.get_table_spark('C_dk')
        if data_dataframe is None:
            data_dataframe = self.dataengine.get_table_spark('T')
        
        dom_prun=domainpruning.DomainPruning(data_dataframe,c_dk_dataframe)
        
        if new_threshold is not None:
            dom_prun.set_threshold(new_threshold)
        
        prunned_domain_dataframe = dom_prun.allowable_doamin_value(self.spark)
        
        self.dataengine.register_spark('D',prunned_domain_dataframe)
        
        return prunned_domain_dataframe

    
    def _featurizer(self,data_dataframe = None):
        """
        This method will fill the X based on the 3 signal that we have in the project
        
        """
        
        pass
    
    def _labeler(self,featurized_dataframe = None,data_dataframe = None):
        """
        This method will fill the Y based on cells in the X
        
        """
        
        pass
    
    def _learn(self,model_name,prunned_domain_dataframe = None,c_clean_dataframe = None):
        """
        
        This method will fill W,b tables to dataset that assigned to dataengine. for learning section it uses model_name to
        specifies which model would be use for now we just implement softmax so model_name='softmax'
        
        :type model_name: string 
        """        
        
        pass
    
    def repair(self,weghts_dataframe = None,bias_dataframe = None,c_dk_dataframe = None,featurized_dataframe = None):
        """
        This method will fill the Y_pred which is a table of probabilities for don't know cells
        this method will call all underscore method for creating Y_pred 
        """
        
        pass
    
    
