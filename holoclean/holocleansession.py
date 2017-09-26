import pyspark as ps
import pandas as pd
import dataengine as de


class HolocleanSession:
    
    def __init__(self,dataengine , spark_master_path = None):
        self.dataengine=dataengine
        if spark_master_path is not None:
            self.spark_master_path=spark_master_path
        self._start_spark_session(spark_master_path)
        
    def _start_spark_session(self,spark_master_path):
        if spark_master_path is None:
            #   spark_master_path would be something like 127.0.0.1:7707           
             self.spark_session=ps.sql.SparkSession.builder.master("local").appName("Holoclean Session").getOrCreate()
        else:
            self.spark_session=ps.sql.SparkSession.builder.master(self.spark_master_path).appName("Holoclean Session").getOrCreate()
           

    def _error_detection(self):
        """
        This method will fill the two table clean and dont-know cell in the dataset that assigned to the dataegine
        
        """
        pass
    
    def _domain_prunnig(self):
        """
        This method will change fill the table D of the dataset that assigned to the dataengine
        
        """
        pass
    
    def _featurizer(self):
        """
        This method will fill the X based on the 3 signal that we have in the project
        
        """
        
        pass
    
    def _labeler(self):
        """
        This method will fill the Y based on cells in the X
        
        """
        
        pass
    
    def _learn(self,model_name):
        """
        
        This method will fill W,b tables to dataset that assigned to dataengine. for learning section it uses model_name to
        specifies which model would be use for now we just implement softemax so model_name='softmax'
        
        :type model_name: string 
        """        
        
        pass
    
    def repair(self):
        """
        This method will fill the Y_pred which is a table of probabilities for don't know cells
        this method will call all underscore method for creating Y_pred
         
        """
        pass
    
    
