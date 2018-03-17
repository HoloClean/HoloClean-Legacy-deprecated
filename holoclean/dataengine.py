#!/usr/bin/env python
import sqlalchemy as sqla
from pyspark.sql.types import *
from utils.reader import Reader


class DataEngine:
    """
    This is the class that contains functionality
    to read the input files and output to MySQL database
    """

    def __init__(self, holo_env):
        """
        The constructor for DataEngine class

        Parameters
        ----------
        :param holo_env: HoloClean
           This parameter is the HoloClean object from the holoclean.py
           module which contains all the connection information.

        Returns
        -------
        No Return
        """

        # Store holoclean environment
        self.holo_env = holo_env

        # Init database backend
        self.db_backend = self._start_db()
        self._db_connect()
        self.sparkSqlUrl = self._init_sparksql_url()
        self.sql_ctxt = self.holo_env.spark_sql_ctxt

        # Init spark dataframe store
        self.spark_dataframes = {}

        # Init Mappings
        self.attribute_map = {}

    # Internal methods
    def _start_db(self):
        """Start MySQL database
        
        Parameters
        ----------
        No parameter
        
        Returns
        -------
        :return: sql_eng : SQL Engine
                This method returns SQL engine to connect to database 
        """
        user = self.holo_env.db_user
        pwd = self.holo_env.db_pwd
        host = self.holo_env.db_host
        dbname = self.holo_env.db_name
        connection = "mysql+mysqldb://" + user + ":" + pwd + \
                     "@" + host + "/" + dbname
        sql_eng = sqla.create_engine(connection)
        return sql_eng

    def _init_sparksql_url(self):
        """
        Creating jdbc url for connection to database

        Parameters
        ----------
        No parameter

        Returns
        -------
        :return: jdbc_url : String
                The string that used for connecting to database
        """
        user = self.holo_env.db_user
        pwd = self.holo_env.db_pwd
        host = self.holo_env.db_host
        dbname = self.holo_env.db_name
        jdbc_url = "jdbc:mysql://" + host + "/" + \
                   dbname + "?user=" + user + "&password=" + \
                   pwd + "&useSSL=false"
        return jdbc_url

    def _db_connect(self):
        """
        Connecting to MySQL database

        Parameters
        ----------
        No parameter

        Returns
        -------
        No Return

        """
        try:
            self.db_backend.connect()
            self.holo_env.logger.info("Connection established to data database")
        except Exception as e:
            self.holo_env.logger.error('No connection to data database',
                                       exc_info=e)
            pass

    def _add_info_to_meta(self, table_name, table_schema, dataset):
        """
        Storing information of a table to the meta table

        Parameters
        ----------
        :param table_name: String
                    The name of table that we want to same its schema in
                    meta table
        :param table_schema: String
                The schema of table table_name
        :param dataset: The data set object to put schema in correct meta table

        Returns
        -------
        :return: table_name_spc : String
                After putting table schema in meta table it returns the
                specific name of the table
        """
        schema = ''
        for attribute in table_schema:
            schema = schema + "," + str(attribute)

        table_name_spc = dataset.table_specific_name(table_name)
        self._add_meta(table_name, schema[1:], dataset)
        return table_name_spc

    def _add_meta(self, table_name, table_schema, dataset):
        """
        Checking if the meta table exists (if not, the code creates it first)
        and add a new row with the information (the id of the data set,
        the name of the table, and its schema) for a new table

        Parameters
        ----------
        :param table_name: The name of table that we want to same its schema in
                            meta table
        :param table_schema: The schema of table table_name
        :param dataset: The data set object to put schema in correct meta table

        Returns
        -------
        No Return
        """
        tmp_conn = self.db_backend.raw_connection()
        dbcur = tmp_conn.cursor()
        stmt = "SHOW TABLES LIKE 'metatable'"
        dbcur.execute(stmt)
        result = dbcur.fetchone()
        add_row = "INSERT INTO metatable (dataset_id, tablename, schem) " \
                  "VALUES('" + dataset.dataset_id + "','" + str(table_name) + \
                  "','" + str(table_schema) + "');"
        if result:
            # There is a table named "metatable"
            self.db_backend.execute(add_row)
        else:
            # create db with columns 'dataset_id' , 'tablename' , 'schem'
            # there are no tables named "metatable"
            create_table = 'CREATE TABLE metatable ' \
                           '(dataset_id TEXT,tablename TEXT,schem TEXT);'
            self.db_backend.execute(create_table)
            self.db_backend.execute(add_row)

    def _table_column_to_dataframe(self, table_name, columns_name_list,
                                   dataset):
        """
        This method gets table general name and returns a spark dataframe
            contains the column of that table

        Parameters
        ----------
        :param table_name: The name of table that we want some of its columns
        :param columns_name_list: The list of columns that we want from
                                table_name
        :param dataset: The data set object to access to the correct project
                        tables

        Returns
        -------
        :return: Dataframe
                It returns a dataframe contains "columns_name_list" columns data
        """
        columns_string = ""
        for c in columns_name_list:
            columns_string += c + ","
        columns_string = columns_string[:-1]
        table_get = "Select " + columns_string + " from " + \
                    dataset.dataset_tables_specific_name[
                        dataset.attributes.index(table_name)]
        use_spark = 1
        return self.query(table_get, use_spark)

    def _dataframe_to_table(self, spec_table_name, dataframe, append=0):
        """
        Adding spark dataframe with specific table name "spec_table_name"
        to the data database with spark session

        Parameters
        ----------
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
        
        Returns
        -------
        No Return
        """
        jdbc_url = "jdbc:mysql://" + self.holo_env.db_host + "/" + \
                   self.holo_env.db_name
        db_properties = {
            "user": self.holo_env.db_user,
            "password": self.holo_env.db_pwd,
            "useSSL": "false",
        }
        if append:
            dataframe.write.jdbc(
                jdbc_url,
                spec_table_name,
                "append",
                properties=db_properties)
        else:
            create_table = "CREATE TABLE " + spec_table_name + " ("
            for i in range(len(dataframe.schema.names)):
                create_table = create_table + " `" + \
                               dataframe.schema.names[i] + "` "
                if dataframe.schema.fields[i].dataType == IntegerType() \
                        or dataframe.schema.names[i] == 'index':
                    create_table = create_table + "INT,"
                else:
                    create_table = create_table + "VARCHAR(255),"
            create_table = create_table[:-1] + " );"
            self.query(create_table)
            self.holo_env.logger.info(create_table)
            self.holo_env.logger.info("  ")
            dataframe.write.jdbc(
                jdbc_url,
                spec_table_name,
                "append",
                properties=db_properties)

    def _query_spark(self, sql_query):
        """
        Executing a query on the MySQL and create a dataframe from the results

        Parameters
        ----------
        :param sql_query: String
                    The query that we want to have it results on the dataframe
                     in the memory

        Returns
        -------
        :return: dataframe : Dataframe
                It returns the results of sql_query in a dataframe
        """
        dataframe = self.sql_ctxt.read.format('jdbc').options(
            url=self._init_sparksql_url(),
            dbtable="(" + sql_query + ") as tablename").load()
        return dataframe

    # Getters
    def get_schema(self, dataset, table_general_name):
        """
        Getting the schema of MySQL table using "table_general_name" and
                "dataset" to identify the table specific name

        Parameters
        ----------
        :param dataset: DataSet
            This parameter is a dataset object used to store the ID
            of the current HoloClean Session
        :param table_general_name: String
            This parameter is the string literal of the table name

        Returns
        -------
        :return: dataframe : String
            If successful will return a string of the schema with the
            column names separated by commas otherwise
            will return "No such element"
        """
        sql_query = "SELECT schem FROM metatable Where dataset_id = '" + \
                    dataset.dataset_id + "' AND  tablename = '" + \
                    table_general_name + "';"

        df = self.query(sql_query, 0)

        try:
            return df.cursor._rows[0][0]
        except Exception as e:
            self.holo_env.logger.error('No such element', exc_info=e)
            return "No such element"

    def get_table_to_dataframe(self, table_name, dataset):
        """
        This method gets table general name and returns a spark dataframe as
         result

        Parameters
        ----------
        :param table_name: String
            string literal of table name not including the session ID
        :param dataset: DataSet
            The DataSet object that holds the Session ID for HoloClean

        Returns
        -------
        :return: dataframe: DataFrame
            The Spark DataFrame representing the MySQL Table
        """

        table_get = "Select * from " + \
                    dataset.dataset_tables_specific_name[
                        dataset.attributes.index(table_name)]

        use_spark = 1
        return self.query(table_get, use_spark)

    def get_db_backend(self):
        """
        This method returns MySQL database
        Parameters
        ----------
        No parameter

        Returns
        -------
        :return: SQL Engine
            This is sql engine returns a sql engine
        """
        return self.db_backend

    # Setters

    def add_db_table(self, table_name, spark_dataframe, dataset, append=0):
        """
        This method get spark dataframe and a table_name and creates a table.

        Parameters
        ----------
        :param table_name: String
            string literal of table name not including the session ID
        :param spark_dataframe: DataFrame
            The dataframe that will be made into a MySQL table
        :param dataset: DataSet
            The DataSet object that holds the Session ID for HoloClean
        :param append: Int
            Optional parameter to specify if we want to
            append dataframe to table or overwrite
            default value is 0 (i.e. overwrite)

        Returns
        -------
        No Return
        """

        schema = spark_dataframe.schema.names
        specific_table_name = self._add_info_to_meta(
            table_name, schema, dataset)
        self._dataframe_to_table(specific_table_name, spark_dataframe, append)

    def add_db_table_index(self, table_name, attr_name):
        """
        This method creates an index to an existing database table.

        Parameters
        ----------
        :param table_name: String
            String literal of table name not including the session ID
        :param attr_name: String
            String literal of the attribute to create an index on

        Returns
        -------
        No Return
        """
        index_id = table_name+"_"+attr_name
        sql = "CREATE INDEX " + index_id + " ON " + table_name + \
              " (" + attr_name + ");"
        self.db_backend.execute(sql)

    def ingest_data(self, filepath, dataset):
        """
        load data from a file to a dataframe and store it on the db

        Parameters
        ----------
        filepath : String
            file path of the .csv file for the dataset
        dataset: DataSet
            The DataSet object that holds the Session ID for HoloClean

        Returns
        -------
        No Return
        """
        # Spawn new reader and load data into dataframe
        file_reader = Reader(self.holo_env.spark_session)
        df = file_reader.read(filepath)

        # Store dataframe to DB table
        schema = df.schema.names
        name_table = self._add_info_to_meta('Init', schema, dataset)
        self._dataframe_to_table(name_table, df)
        table_attribute_string = self.get_schema(
            dataset, "Init")
        count = 0
        map_schema = []
        attributes = table_attribute_string.split(',')
        for attribute in attributes:
            if attribute != "index":
                count = count + 1
                map_schema.append([count, attribute])

        dataframe_map_schema = self.holo_env.spark_session.createDataFrame(
            map_schema, StructType([
                StructField("index", IntegerType(), False),
                StructField("attribute", StringType(), True)
            ]))
        self.add_db_table('Map_schema', dataframe_map_schema, dataset)

        for table_tuple in map_schema:
            self.attribute_map[table_tuple[1]] = table_tuple[0]
        return

    def query(self, sql_query, spark_flag=0):
        """
        execute a query, uses the flag to decide if it will store the results
        on spark dataframe

        Parameters
        ----------
        :param sql_query: String
            string literal of sql query to be executed
        :param spark_flag: Int
            Default value: 0
            If 1 will use Pyspark otherwise will use SqlAlchemy

        Returns
        -------
        :return: dataframe: DataFrame
            The DataFrame representing the result of the query if
            spark_flag = 0
            otherwise None


        """

        if spark_flag == 1:
            return self._query_spark(sql_query)
        else:
            return self.db_backend.execute(sql_query)
