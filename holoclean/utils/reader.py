import pandas as pd
import numpy as np


    

class Reader:
    
    """Finds the extesion of the file and calls the appropriate reader

    Takes as argument the full path name of the  file
    """
    chunksize = 20000
    
    def __init__(self,filepath):
        self.filepath=filepath
    
    def findextesion(self):
        """Finds the extesion of the file.

        Takes as argument the full path name of the file
         """
        extention = self.filepath.split('.')[-1]
        return extention

    def reader(self,dataengine):
        """Calls the appropriate reader for the file

        Takes as argument the full path name of the  file
        """
        if (self.findextesion() == "csv"):
            csv_obj = CSVReaders(self.filepath,self.chunksize)
            csv_obj.csv_reader(dataengine)
        
        ##### More Extension will come in here ######
        else :
            print("This extension doesn't support")
    

    
    def reader_spark(self,dataengine,spark_session):
        """Calls the appropriate reader for the file

        Takes as argument the full path name of the  file
        """
        if (self.findextesion() == "csv"):
            csv_obj = CSVReaders_spark(self.filepath,spark_session)
            df=csv_obj.csv_reader(dataengine)
            return df
        
        ##### More Extension will come in here ######
        else :
            print("This extension doesn't support")

    def reader_cursor(self,dataengine):
        """Calls the appropriate reader for the file

        Takes as argument the full path name of the  file
        """
        if (self.findextesion() == "csv"):
            csv_obj = CSVReaders_cursor(self.filepath)
            csv_obj.csv_reader(dataengine)
        
        ##### More Extension will come in here ######
        else :
            print("This extension doesn't support")
    

class CSVReaders:
    
    def __init__(self,file_path,chunksize):
        self.file_path=file_path
        self.chunksize=chunksize

    def csv_reader(self,dataengine):
        """Creates a chunksize of size 20000, reads the csv files and sends the chunk
           to the DataEgine.

        Takes as argument the full path name of the csv file
        """
        
        #data.connect()
        first_time = 0
        for chunk in pd.read_csv(self.file_path, chunksize=self.chunksize):
            if first_time == 0:
                name_table = dataengine._csv2DB('T',chunk)
                first_time = first_time + 1
            else:
                dataengine.add(name_table , chunk)

class CSVReaders_spark:
    
    def __init__(self,file_path,spark_session):
        self.file_path=file_path
        self.spark_session=spark_session

    def csv_reader(self,dataengine):
        """Creates a chunksize of size 20000, reads the csv files and sends the chunk
           to the DataEgine.

        Takes as argument the full path name of the csv file
        """
        
    	df=self.spark_session.read.csv("10.csv",header=True)
    	schema=df.schema.names
    	name_table=dataengine._csv2DB_spark('T',schema)
    	dataengine._add_spark(name_table, df)
        
        return df

class CSVReaders_cursor:
    
    def __init__(self,file_path):
        self.file_path=file_path

    def csv_reader(self,dataengine):
        """Creates a chunksize of size 20000, reads the csv files and sends the chunk
           to the DataEgine.

        Takes as argument the full path name of the csv file
        """
        schema=""
        name_table=dataengine._csv2DB_cursor('T',schema)
        dataengine.add_cursor(name_table,"/var/lib/mysql-files/10.csv")

