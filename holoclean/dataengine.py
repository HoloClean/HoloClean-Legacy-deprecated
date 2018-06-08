#!/usr/bin/env python
from sys import exit
from pyspark.sql.types import *
from global_variables import GlobalVariables
from utils.reader import Reader
import psycopg2


class DataEngine:
    """
    This is the class that contains functionality
    to read the input files and output to Postgres database
    """

    def __init__(self, holo_env):
        """
        The constructor for DataEngine class

        :param holo_env: HoloClean
           The HoloClean object from holoclean.py
           module which contains all the connection information.

        """

        # Store holoclean environment
        self.holo_env = holo_env

        # Init database backend
        self.db_backend = self._start_db()
        self.sparkSqlUrl = self._init_sparksql_url()
        self.sql_ctxt = self.holo_env.spark_sql_ctxt

        # Init spark dataframe store
        self.spark_dataframes = {}

        # Init Mappings
        self.attribute_map = {}

    # Internal methods
    def _start_db(self):

        """
        Start Postgres database

        :return: cur
                Cursor for sql engine
        :return: conn
                Connection to database

        """

        user = self.holo_env.db_user
        pwd = self.holo_env.db_pwd
        host = self.holo_env.db_host
        dbname = self.holo_env.db_name

        connection_string = "dbname= '" + dbname + "' user='" + user + \
                            "' host='" + host + "' password='" + pwd + "'"
        try:
            conn = psycopg2.connect(connection_string)

        except Exception as e:
            self.holo_env.logger.\
                error('No connection to data database', exc_info=e)
            exit(1)

        cur = conn.cursor()
        return cur, conn

    def _init_sparksql_url(self):
        """
        Creating jdbc url for connection to database

        :return: jdbc_url : String
                The string that used for connecting to database
        :return: db_properties : Dictionary
                Dictionary have all properties
        """
        user = self.holo_env.db_user
        pwd = self.holo_env.db_pwd
        host = self.holo_env.db_host
        dbname = self.holo_env.db_name

        jdbc_url = "jdbc:postgresql://" + host + "/" + dbname

        db_properties = {
            "user": user,
            "password": pwd,
            "ssl": "false",
        }

        return jdbc_url, db_properties

    def _query_spark(self, sql_query):
        """
        Executing a query on the MySQL and create a dataframe from the results

        :param sql_query: String
                    Query string to execute

        :return: dataframe : Dataframe
                The results of sql_query in a dataframe
        """

        url = self.sparkSqlUrl
        dataframe = self.sql_ctxt.read.jdbc(
            url=url[0], table="(" + sql_query + ") as tablename",
            properties=url[1])

        return dataframe

    # Getter

    def get_table_to_dataframe(self, table_name, dataset):
        """
        Getting a table general name and returns a spark dataframe as
         result

        :param table_name: String
            String literal of table name not including the session ID
        :param dataset: DataSet
            The DataSet object that holds the Session ID for HoloClean

        :return: dataframe: DataFrame
            The Spark DataFrame representing the MySQL Table
        """

        table_get = "Select * from " + \
                    dataset.table_specific_name(table_name)

        use_spark = 1
        return self.query(table_get, use_spark)

    def get_db_backend(self):
        """
        Returns Postgres database

        :return: SQL Engine
        """
        return self.db_backend

    def query(self, sql_query, spark_flag=0):
        """
        Executing a query, uses the flag to decide if it will store the results
        on spark dataframe

        :param sql_query: String
            String literal of sql query to be executed
        :param spark_flag: Int
            Default value: 0
            If 1 will use Pyspark otherwise will call Postgres connection

        :return: dataframe: DataFrame if spark_flag = 1
            otherwise None

        """
        try:
            if spark_flag == 1:
                return self._query_spark(sql_query)
            else:
                result = self.db_backend[0].execute(sql_query)
                self.db_backend[1].commit()
                return result
        except Exception as e:
            self.holo_env.logger.error('Could not execute Query' + sql_query,
                                       exc_info=e)
            print "Could not execute Query ", sql_query, "Check log for info"
            self.db_backend[1].abort()
            exit(5)

    def ingest_data(self, filepath, dataset):
        """
        Load data from a file to a dataframe and store it on the db

        filepath : String
            File path of the .csv file for the dataset
        dataset: DataSet
            The DataSet object that holds the Session ID for HoloClean

        """

        # Spawn new reader and load data into dataframe
        filereader = Reader(self.holo_env.spark_session)

        # read with an index column
        df = filereader.read(filepath,1)

        # Store dataframe to DB table
        schema = df.schema.names
        name_table = dataset.table_specific_name('Init')
        self.dataframe_to_table(name_table, df)
        dataset.attributes['Init'] = schema
        count = 0
        map_schema = []
        attribute_map = {}
        for attribute in schema:
            if attribute != GlobalVariables.index_name:
                count = count + 1
                map_schema.append([count, attribute])
                attribute_map[attribute] = count

        dataframe_map_schema = self.holo_env.spark_session.createDataFrame(
            map_schema, dataset.attributes['Map_schema'])
        self.add_db_table('Map_schema', dataframe_map_schema, dataset)

        for table_tuple in map_schema:
            self.attribute_map[table_tuple[1]] = table_tuple[0]

        return df, attribute_map

    # Setter

    def dataframe_to_table(self, spec_table_name, dataframe, append=0):
        """
        Adding spark dataframe with specific table name "spec_table_name"
        to the data database with spark session

        :param spec_table_name: String
                    The specific name of table that we want to put "dataframe"
                     into it
        :param dataframe: Dataframe
                The name of data name that we want to add it information into
                 "spec_table_name"
        :param append: Int
                If this parameter equal to zero, the code first creates a
                table, then it adds "dataframe" information into table,
                otherwise it just appends "dataframe" into "spec_table_name"

        """
        jdbc_url = self.sparkSqlUrl

        if append:
            dataframe.write.jdbc(
                jdbc_url[0],
                spec_table_name,
                "append",
                properties=jdbc_url[1])
        else:
            create_table = "CREATE TABLE " + spec_table_name + " ("
            for i in range(len(dataframe.schema.names)):

                create_table = create_table + " " + \
                               dataframe.schema.names[i] + " "

                if dataframe.schema.fields[i].dataType == IntegerType()\
                        or dataframe.schema.names[i] == \
                        GlobalVariables.index_name:

                    create_table = create_table + "INT,"

                elif dataframe.schema.fields[i].dataType == DoubleType() \
                        or dataframe.schema.names[i] == \
                        GlobalVariables.index_name:

                    create_table = create_table + "DOUBLE PRECISION,"

                else:

                    create_table = create_table + "VARCHAR(255),"

            if GlobalVariables.index_name in dataframe.schema.names:
                create_table = \
                    create_table + \
                    " PRIMARY KEY (" + GlobalVariables.index_name + ") "
            create_table = create_table[:-1] + " );"
            self.query(create_table)
            self.holo_env.logger.info(create_table)
            self.holo_env.logger.info("  ")
            dataframe.write.jdbc(
                jdbc_url[0],
                spec_table_name,
                "append",
                properties=jdbc_url[1])

    def add_db_table(self, table_name, spark_dataframe, dataset, append=0):
        """
        This method get spark dataframe and a table_name and creates a table.

        :param table_name: String
            String literal of table name not including the session ID
        :param spark_dataframe: DataFrame
            The dataframe that will be made into a MySQL table
        :param dataset: DataSet
            The DataSet object that holds the Session ID for HoloClean
        :param append: Int
            Optional parameter to specify if we want to
            append dataframe to table or overwrite
            default value is 0 (i.e. overwrite)
        """
        specific_table_name = dataset.table_specific_name(table_name)
        self.dataframe_to_table(specific_table_name, spark_dataframe, append)

    def add_db_table_index(self, table_name, attr_name):
        """
        This method creates an index to an existing database table.

        :param table_name: String
            String literal of table name not including the session ID
        :param attr_name: String
            String literal of the attribute to create an index on

        """
        index_id = table_name+"_"+attr_name
        sql = "CREATE INDEX " + index_id + " ON " + table_name + \
              " (" + attr_name + ");"
        self.db_backend[0].execute(sql)
        self.db_backend[1].commit()
