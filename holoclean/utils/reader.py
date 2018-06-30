from holoclean.global_variables import GlobalVariables
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, LongType


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
        try:
            if schema is None:
                df = spark_session.read.csv(file_path, header=True)
            else:
                df = spark_session.read.csv(file_path, header=True, schema=schema)

            if indexcol == 0:
                return df
        except Exception as e:
            print("File not found")
            exit(1)

        index_name = GlobalVariables.index_name

        new_cols = df.schema.names + [index_name]
        list_schema = []
        for index_attribute in range(len(df.schema.names)):
            list_schema.append(StructField("_" + str(index_attribute),
                                           df.schema[
                                               index_attribute].dataType,
                                           True))
        list_schema.append(
            StructField("_" + str(len(new_cols)), LongType(), True))

        schema = StructType(list_schema)
        ix_df = df.rdd.zipWithIndex().map(
            lambda (row, ix): row + (ix + 1,)).toDF(schema)
        tmp_cols = ix_df.schema.names
        new_df = reduce(lambda data, idx: data.withColumnRenamed(tmp_cols[idx],
                        new_cols[idx]),
                        xrange(len(tmp_cols)), ix_df)
        new_df = self.checking_string_size(new_df)
        return new_df

    def checking_string_size(self, dataframe):
        """
        This method checks if the dataframe has  columns with strings with more
        than 255 characters

        :param dataframe:  the initial dataframe
        :return: dataframe: a new dataframe without the columns with strings
        with more than 255 characters
        """

        columns = set([])
        for row in dataframe.collect():
            for attribute in dataframe.columns:
                if isinstance(row[attribute], unicode) and\
                       len(row[attribute]) > 255:
                    columns.add(attribute)
        if len(columns) > 0:
            dataframe = self.ignore_columns(columns, dataframe)
        return dataframe

    def ignore_columns(self,  columns, dataframe):
        """
        This method asks the user if he wants to drop a column which has a
        string with more than 255 characters

        :param  columns: a set of columns with strings with more
        than 255 characters
        :param dataframe: the dataframe that we want to change

        :return: dataframe: a new dataframe
        """
        print("Holoclean cannot use dataframes with strings "
              "more than 255 characters")
        columns_message = " and ".join(columns)
        message = "The columns " + columns_message + " will be dropped"
        print (message)
        for column in columns:
                dataframe = dataframe.drop(column)
        return dataframe



