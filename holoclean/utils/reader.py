class Reader:

    """Reader class:
    Finds the extension of the file and calls the appropriate reader
    """

    def __init__(self, spark_session):
        """
-
        :param spark_session: The spark_session we created in Holoclean object
        """
        self.spark_session = spark_session

    # Internal Methods
    def _findextesion(self, filepath):
        """Finds the extesion of the file.

        :param filepath: The path to the file
        """
        extention = filepath.split('.')[-1]
        return extention

    # Setters
    def read(self, filepath):
        """Calls the appropriate reader for the file

        :param filepath: The path to the file
        """
        if (self._findextesion(filepath) == "csv"):
            csv_obj = CSVReader()
            df = csv_obj.read(filepath, self.spark_session)
            return df
        else:
            print("This extension doesn't support")


class CSVReader:
    """CSVReader class: Reads a csv file and send its content back"""

    def __init__(self):
        pass

    # Setters
    def read(self, file_path, spark_session):
        """Create a dataframe from the csv file

        :param spark_session: The spark_session we created in Holoclean object
        :param file_path: The path to the file

        """
        df = spark_session.read.csv(file_path, header=True)

        return df
