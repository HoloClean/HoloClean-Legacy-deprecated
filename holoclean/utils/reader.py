class Reader:

    """TODO:Reader class:Finds the extesion of the file and calls the
    appropriate reader"""

    def __init__(self, spark_session):
        """TODO.
        Parameters
        --------
        parameter: spark_session
                Takes as an argument the spark_Session from the Data Engine
        """
        self.spark_session = spark_session

    # Internal Methods
    def _findextesion(self, filepath):
        """Finds the extesion of the file.

        Takes as argument the full path name of the file
         """
        extention = filepath.split('.')[-1]
        return extention

    # Setters
    def read(self, filepath):
        """Calls the appropriate reader for the file

        Takes as argument the full path name of the  file
        """
        if (self._findextesion(filepath) == "csv"):
            csv_obj = CSVReader()
            df = csv_obj.read(filepath, self.spark_session)
            return df
        else:
            print("This extension doesn't support")


class CSVReader:
    """TODO:CSVReader class: Reads a csv file and send its content back"""

    def __init__(self):
        pass

    # Setters
    def read(self, file_path, spark_session):
        """Create a dataframe from the csv file

        Takes as argument the full path name of the csv file and the
        spark_session
        """
        df = spark_session.read.csv(file_path, header=True)

        return df
