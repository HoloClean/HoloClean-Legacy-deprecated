import pandas as pd
import numpy as np

class Ingest:

    """Finds the extesion of the file and calls the appropriate reader

    Takes as argument the full path name of the  file
    """

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
            self.csv_reader(dataengine)

    def csv_reader(self,dataengine):
        """Creates a chunksize of size 20000, reads the csv files and sends the chunk
           to the DataEgine.

        Takes as argument the full path name of the csv file
        """
        chunksize = 20000
        #data.connect()
        first_time = 0
        for chunk in pd.read_csv(self.filepath, chunksize=chunksize):
            if first_time == 0:
                name_table = dataengine.register(chunk)
                first_time = first_time + 1
            else:
                dataengine.add(chunk, name_table)
