from holoclean.global_variables import GlobalVariables
from pyspark.sql.types import *
from pyspark.sql.functions import *

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
        """Creates a dataframe from the csv file

        :param spark_session: The spark_session we created in Holoclean object
        :param file_path: The path to the file

        """
        df = spark_session.read.csv(file_path, header=True)
        index_name = GlobalVariables.index_name

        new_cols = df.schema.names + [index_name]
        ix_df = df.rdd.zipWithIndex().map(lambda (row, ix): row + (ix + 1,)).toDF()
        tmp_cols = ix_df.schema.names
        new_df = reduce(lambda data, idx: data.withColumnRenamed(tmp_cols[idx],
                        new_cols[idx]),
                        xrange(len(tmp_cols)), ix_df)

        new_df = self.clean_up_dataframe(new_df)

        return new_df

    def clean_up_dataframe(self, df):
        """

        :param df: a dataframe that we want to change
        :return: a new dataframe where have clean up its context
        """
        df.toDF(*[c.lower() for c in df.columns])
        list_attributes = df.schema.names
        index_name = GlobalVariables.index_name

        for attribute in list_attributes:
            if attribute != index_name:
                df = df.withColumn(
                    attribute, regexp_replace(attribute, '  +', ' '))
                df = df.withColumn(
                    attribute, regexp_replace(attribute, '\n', ''))
                df = df.withColumn(
                    attribute, regexp_replace(attribute, '"', ''))
                df = df.withColumn(
                    attribute, regexp_replace(attribute, "'", ''))
                new_df = df.withColumn(attribute, trim(col(attribute)))
        return new_df
