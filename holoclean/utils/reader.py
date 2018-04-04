from holoclean.global_variables import GlobalVariables
from pyspark.sql.functions import *


class Reader:

    """
    Reader class:
    Finds the extension of the file and calls the appropriate reader
    """

    def __init__(self, spark_session):
        """
        Constructing reader object

        :param spark_session: The spark_session we created in Holoclean object
        """
        self.spark_session = spark_session

    # Internal Methods
    def _findextesion(self, filepath):
        """
        Finds the extesion of the file.

        :param filepath: The path to the file
        """
        extention = filepath.split('.')[-1]
        return extention

    def read(self, filepath, indexcol=0, schema=None):
        """
        Calls the appropriate reader for the file

        :param schema: optional schema when known
        :param filepath: The path to the file

        :return: data frame of the read data

        """
        if self._findextesion(filepath) == "csv":
            csv_obj = CSVReader()
            df = csv_obj.read(filepath, self.spark_session, indexcol, schema)
            return df
        else:
            print("This extension doesn't support")


class CSVReader:
    """
    CSVReader class: Reads a csv file and send its content back
    """

    def __init__(self):
        pass

    # Setters
    def read(self, file_path, spark_session, indexcol=0, schema=None):
        """
        Creates a dataframe from the csv file

        :param indexcol: if 1, create a tuple id column as auto increment
        :param schema: optional schema of file if known
        :param spark_session: The spark_session we created in Holoclean object
        :param file_path: The path to the file

        :return: dataframe
        """
        if schema is None:
            df = spark_session.read.csv(file_path, header=True)
        else:
            df = spark_session.read.csv(file_path, header=True, schema=schema)

        if indexcol == 0:
            return df

        index_name = GlobalVariables.index_name
        new_cols = df.schema.names + [index_name]
        ix_df = df.rdd.zipWithIndex().map(
            lambda (row, ix): row + (ix + 1,)).toDF()
        tmp_cols = ix_df.schema.names
        new_df = reduce(lambda data, idx: data.withColumnRenamed(tmp_cols[idx],
                        new_cols[idx]),
                        xrange(len(tmp_cols)), ix_df)
        return new_df



