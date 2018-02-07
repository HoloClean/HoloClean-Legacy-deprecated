#!/usr/bin/env python
import sqlalchemy as sqla
import mysql.connector
from dataset import *
import pandas as pd
from pyspark.sql.types import *
from utils.reader import Reader


class DataEngine:
    """TODO: Data Engine Class."""

    def __init__(self, holoEnv):
        """TODO.

        Parameters
        ----------
        parameter : type
           This is a parameter

        Returns
        -------
        describe : type
            Explanation
        """

        # Store holoclean environment
        self.holoEnv = holoEnv

        # Init database backend
        self.db_backend = self._start_db()
        self._db_connect()
        self.sparkSqlUrl = self._init_sparksql_url()
        self.sql_ctxt = self.holoEnv.spark_sql_ctxt

        # Init spark dataframe store
        self.spark_dataframes = {}

        # Init Mappings
        self.attribute_map = {}

    # Internal methods
    def _start_db(self):
        """TODO: Start MySQL database"""
        user = self.holoEnv.db_user
        pwd = self.holoEnv.db_pwd
        host = self.holoEnv.db_host
        dbname = self.holoEnv.db_name
        connection = "mysql+mysqldb://" + user + ":" + pwd + "@" + host + "/" + dbname
        return sqla.create_engine(connection)

    def _init_sparksql_url(self):
        """TODO: Start MySQL database"""
        user = self.holoEnv.db_user
        pwd = self.holoEnv.db_pwd
        host = self.holoEnv.db_host
        dbname = self.holoEnv.db_name
        jdbcUrl = "jdbc:mysql://" + host + "/" + \
            dbname + "?user=" + user + "&password=" + pwd + "&useSSL=false"
        return jdbcUrl

    def _db_connect(self):
        """Connect to MySQL database"""
        try:
            self.db_backend.connect()
            self.holoEnv.logger.info("Connection established to data database")
        except BaseException:
            self.holoEnv.logger.warn("No connection to data database")
            pass

    def _add_info_to_meta(self, table_name, table_schema, dataset):
        """
        TO DO:store information for a table to the metatable
        """

        schema = ''
        for attribute in table_schema:
            schema = schema + "," + str(attribute)

        table_name_spc = dataset.table_specific_name(table_name)
        self._add_meta(table_name, schema[1:], dataset)
        return table_name_spc

    def _add_meta(self, table_name, table_schema, dataset):
        """
        TO DO:checks if the metatable exists (if not it is created) and add a new row with the informations
        (the id of the dataset, the name of the table and the schema) for a new table
        """
        tmp_conn = self.db_backend.raw_connection()
        dbcur = tmp_conn.cursor()
        stmt = "SHOW TABLES LIKE 'metatable'"
        dbcur.execute(stmt)
        result = dbcur.fetchone()
        add_row = "INSERT INTO metatable (dataset_id,tablename,schem) VALUES('" + \
            dataset.dataset_id + "','" + str(table_name) + "','" + str(table_schema) + "');"
        if result:
            # there is a table named "metatable"
            self.db_backend.execute(add_row)
        else:
            # create db with columns 'dataset_id' , 'tablename' , 'schem'
            # there are no tables named "metatable"
            create_table = 'CREATE TABLE metatable (dataset_id TEXT,tablename TEXT,schem TEXT);'
#             dbcur.execute(create_table)
            self.db_backend.execute(create_table)
            self.db_backend.execute(add_row)

    def _table_column_to_dataframe(self, table_name, columns_name_list, dataset):
        """
        This method get table general name and return it as spark dataframe
        """
        columns_string=""
        for c in columns_name_list:
            columns_string += c + ","
        columns_string = columns_string[:-1]
        table_get = "Select " + columns_string + " from " + dataset.dataset_tables_specific_name[
            dataset.attributes.index(table_name)]
        useSpark = 1
        return self.query(table_get, useSpark)

    def _dataframe_to_table(self, spec_table_name, dataframe):
        """Add spark dataframe df with specific name table name_table in the data database
        with spark session
        """

        jdbcUrl = "jdbc:mysql://" + self.holoEnv.db_host + "/" + self.holoEnv.db_name
        dbProperties = {
            "user": self.holoEnv.db_user,
            "password": self.holoEnv.db_pwd,
        }

        dataframe.write.jdbc(
            jdbcUrl,
            spec_table_name,
            "overwrite",
            properties=dbProperties)

    def _query_spark(self, sqlQuery):
        """
        TO DO:execute a query and create a dataframe from the results
        """

        dataframe = self.sql_ctxt.read.format('jdbc').options(
            url=self._init_sparksql_url(),
            dbtable="(" + sqlQuery + ") as tablename").load()
        return dataframe


    # Getters
    def get_schema(self, dataset, table_general_name):

        sql_query = "SELECT schem FROM metatable Where dataset_id = '" + \
            dataset.dataset_id + "' AND  tablename = '" + table_general_name + "';"
        mt_eng = self.db_backend

        generator = pd.read_sql_query(sql_query, mt_eng)
        dataframe = pd.DataFrame(generator)

        try:
            return dataframe.iloc[0][0]
        except BaseException:
            return "Not such element"

    def get_table_to_dataframe(self, table_name, dataset):
        """
        This method get table general name and return it as spark dataframe
        """

        table_get = "Select * from " + \
                    dataset.dataset_tables_specific_name[dataset.attributes.index(table_name)]

        useSpark = 1

        return self.query(table_get, useSpark)

    def get_db_backend(self):
        """Return MySQL database"""
        return self.db_backend

    # Setters

    # Will set the column datatype of column_name to INTEGER using a MySQL statement
    def altar_column(self, dataset, column_name):
        sql_statement="ALTER TABLE " + dataset.table_specific_name('Init') \
                  + " MODIFY COLUMN " + column_name + " INT;"
        self.db_backend.execute(sql_statement)

    def add_db_table(self, table_name, spark_dataframe, dataset):
        """
        This method get spark dataframe and a table_name and creates a table.
        """

        schema = spark_dataframe.schema.names
        specific_table_name = self._add_info_to_meta(
            table_name, schema, dataset)
        self._dataframe_to_table(specific_table_name, spark_dataframe)

    def ingest_data(self, filepath, dataset):
        """
        TO DO:load data from a file to a dataframe and store it on the db
        """
        # Spawn new reader and load data into dataframe
        fileReader = Reader(self.holoEnv.spark_session)
        df = fileReader.read(filepath)

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


        dataframe_map_schema = self.holoEnv.spark_session.createDataFrame(
        map_schema, StructType([
                StructField("index", IntegerType(), False),
                StructField("attribute", StringType(), True)
            ]))
        self.add_db_table('Map_schema', dataframe_map_schema, dataset)

        for tuple in map_schema:
            self.attribute_map[tuple[1]] = tuple[0]
        return

    def query(self, sqlQuery, spark_flag=0):
        """
        TO DO:execute a query, uses the flag to decide if it will store the results on spark dataframe
        """

        if spark_flag == 1:
            return self._query_spark(sqlQuery)
        else:
            return self.db_backend.execute(sqlQuery)


