import pandas as pd
import numpy as np
import Dataengine as dt


class ingest:

    """Finds the extesion of the file and calls the appropriate reader

    Takes as argument the full path name of the  file
    """

    def __init__(self):
        pass

    def findextesion(self, filepath):
        """Finds the extesion of the file.

        Takes as argument the full path name of the file
         """
        extention = filepath.split('.')[-1]
        return extention

    def reader(self, filepath):
        """Calls the appropriate reader for the file

        Takes as argument the full path name of the  file
        """
        if (self.findextesion(filepath) == "csv"):
            self.csv_reader(filepath)

    def csv_reader(self, filepath):
        """Creates a chunksize of size 20000, reads the csv files and sends the chunk
           to the DataEgine.

        Takes as argument the full path name of the csv file
        """
        chunksize = 20000
        data = dt.dataengine()
        data.connect()
        first_time = 0
        for chunk in pd.read_csv(filepath, chunksize=chunksize):
            if first_time == 0:
                name_table = data.register(chunk)
                first_time = first_time + 1
            else:
                data.add(chunk, name_table)
