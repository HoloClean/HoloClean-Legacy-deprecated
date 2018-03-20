from holoclean.utils.dcparser import DCParser
from errordetector import ErrorDetection
import time
from pyspark.sql.types import *



__metaclass__ = type


class MysqlDCErrorDetection(ErrorDetection):
    """
    This class is a subclass of the errordetector class and
    will return  error  cells and clean cells based on the
    denial constraint
    """

    def __init__(self, session):
        """
        This constructor  converts all denial constraints
        to the form of SQL constraints

        :param session: Holoclean session
        """
        super(MysqlDCErrorDetection, self).__init__(session.holo_env,
                                                    session.dataset)
        self.dc_parser = session.parser
        self.all_dcs = self.dc_parser.get_CNF_of_dcs(session.Denial_constraints)
        self.operationsarr = DCParser.operationsArr
        self.noisy_cells = None
        self.dictionary_dc = self.dc_parser.create_dc_map(self.all_dcs)

    def _is_symetric(self, dc_name):
        result = True
        non_sym_ops = [ '<=', '>=', '<', '>', ]
        for op in non_sym_ops:
            result = False
        return result


    # Getters

    def get_noisy_cells_for_dc(self, dc_name):
        """
                Return a dataframe that consist of index of noisy cells index,
                attribute

                :return: spark_dataframe
                """

        self.holo_obj.logger.info('Denial Constraint Queries For ' + dc_name)
        temp_table = "tmp" + self.dataset.dataset_id
        query = "CREATE TABLE " + temp_table + " AS SELECT " \
                                               "t1.index as t1_ind, " \
                                               "t2.index as t2_ind " \
                " FROM  " + \
                self.dataset.table_specific_name("Init") + \
                " as t1, " + \
                self.dataset.table_specific_name("Init") + \
                " as  t2 " + "WHERE t1.index != t2.index  AND " + dc_name
        self.dataengine.query(query)

        t1_attributes = set()
        t2_attributes = set()

        dc_predicates = self.dictionary_dc[dc_name]
        for predicate_index in range(0, len(dc_predicates)):
            predicate_type = dc_predicates[predicate_index][4]
            # predicate_type 0 : we do not have a literal in this predicate
            # predicate_type 1 : literal on the left side of the predicate
            # predicate_type 2 : literal on the right side of the predicate
            if predicate_type == 0:
                relax_indices = range(2, 4)
            elif predicate_type == 1:
                relax_indices = range(3, 4)
            elif predicate_type == 2:
                relax_indices = range(2, 3)
            else:
                raise ValueError(
                    'predicate type can only be 0: '
                    'if the predicate does not have a literal'
                    '1: if the predicate has a literal in the left side,'
                    '2: if the predicate has a literal in right side'
                )
            for relax_index in relax_indices:
                name_attribute = \
                    dc_predicates[predicate_index][relax_index].split(".")
                if name_attribute[0] == "t1":
                    t1_attributes.add(name_attribute[1])
                elif name_attribute[0] == "t2":
                    t2_attributes.add(name_attribute[1])

        left_attributes = [[i] for i in t1_attributes]
        right_attributes = [[i] for i in t2_attributes]

        t1_attributes_dataframe = self.spark_session.createDataFrame(
            left_attributes, ['attr_name'])

        t2_attributes_dataframe = self.spark_session.createDataFrame(
            right_attributes, ['attr_name'])

        t1_name = self.dataset.table_specific_name("T1_attributes")
        t2_name = self.dataset.table_specific_name("T2_attributes")
        self.dataengine._dataframe_to_table(t1_name, t1_attributes_dataframe)
        self.dataengine._dataframe_to_table(t2_name, t2_attributes_dataframe)

        # Left part of predicates
        distinct_left = \
            "(SELECT DISTINCT t1_ind  FROM " + temp_table + ") AS row_table"

        query_left = "INSERT INTO " + \
                     self.dataset.table_specific_name("C_dk_temp") + \
                     " SELECT row_table.t1_ind as ind, a.attr_name as attr FROM " + \
                     t1_name + \
                     " AS a," + \
                     distinct_left
        self.dataengine.query(query_left)

        self.holo_obj.logger.info('Denial Constraint Query Left ' +
                                  dc_name + ":" + query_left)

        # Right part of predicates
        distinct_right = \
            "(SELECT DISTINCT t2_ind  FROM " + temp_table + ") AS row_table"
        query_right = "INSERT INTO " + \
                      self.dataset.table_specific_name("C_dk_temp") + \
                      " SELECT row_table.t2_ind as ind, " \
                      "a.attr_name as attr " \
                      "FROM " + \
                      t2_name + " AS a," + distinct_right

        self.dataengine.query(query_right)

        self.holo_obj.logger.info('Denial Constraint Query Right ' +
                                  dc_name + ":" + query_right)

        drop_temp_table = "DROP TABLE " + temp_table + ";DROP TABLE " + \
                          t1_name + ";DROP TABLE " + t2_name
        self.dataengine.query(drop_temp_table)

    def get_sym_noisy_cells_for_dc(self, dc_name):
        """
                Return a dataframe that consist of index of noisy cells index,
                attribute

                :return: spark_dataframe
                """

        self.holo_obj.logger.info('Denial Constraint Queries For ' + dc_name)
        temp_table = "tmp" + self.dataset.dataset_id
        query = "CREATE TABLE " + temp_table + " AS SELECT " \
                                               "t1.index as t1_ind, " \
                                               "t2.index as t2_ind " \
                " FROM  " + \
                self.dataset.table_specific_name("Init") + \
                " as t1, " + \
                self.dataset.table_specific_name("Init") + \
                " as  t2 " + "WHERE t1.index != t2.index  AND " + dc_name
        self.dataengine.query(query)

        t1_attributes = set()

        dc_predicates = self.dictionary_dc[dc_name]
        for predicate_index in range(0, len(dc_predicates)):
            predicate_type = dc_predicates[predicate_index][4]
            # predicate_type 0 : we do not have a literal in this predicate
            # predicate_type 1 : literal on the left side of the predicate
            # predicate_type 2 : literal on the right side of the predicate
            if predicate_type == 0:
                relax_indices = range(2, 4)
            elif predicate_type == 1:
                relax_indices = range(3, 4)
            elif predicate_type == 2:
                relax_indices = range(2, 3)
            else:
                raise ValueError(
                    'predicate type can only be 0: '
                    'if the predicate does not have a literal'
                    '1: if the predicate has a literal in the left side,'
                    '2: if the predicate has a literal in right side'
                )
            for relax_index in relax_indices:
                name_attribute = \
                    dc_predicates[predicate_index][relax_index].split(".")
                if name_attribute[0] == "t1":
                    t1_attributes.add(name_attribute[1])

        left_attributes = [[i] for i in t1_attributes]

        t1_attributes_dataframe = self.spark_session.createDataFrame(
            left_attributes, ['attr_name'])


        t1_name = self.dataset.table_specific_name("T1_attributes")
        self.dataengine._dataframe_to_table(t1_name, t1_attributes_dataframe)

        # Left part of predicates
        distinct_left = \
            "(SELECT DISTINCT t1_ind  FROM " + temp_table + ") AS row_table"

        query_left = "INSERT INTO " + \
                     self.dataset.table_specific_name("C_dk_temp") + \
                     " SELECT row_table.t1_ind as ind, a.attr_name as attr FROM " + \
                     t1_name + \
                     " AS a," + \
                     distinct_left
        self.dataengine.query(query_left)

        self.holo_obj.logger.info('Denial Constraint Query Left ' +
                                  dc_name + ":" + query_left)


        drop_temp_table = "DROP TABLE " + temp_table
        self.dataengine.query(drop_temp_table)

    def get_noisy_cells(self):
        """
        Return a dataframe that consist of index of noisy cells index,attribute

        :return: spark_dataframe
        """

        table_name = self.dataset.table_specific_name("C_dk_temp")
        query_for_creation_table = "CREATE TABLE " + table_name + \
                                   "(ind INT, attr VARCHAR(255));"
        self.dataengine.query(query_for_creation_table)
        for dc_name in self.dictionary_dc:
            self.get_noisy_cells_for_dc(dc_name)

        c_dk_drataframe = self.dataengine.\
            get_table_to_dataframe("C_dk_temp", self.dataset)
        self.noisy_cells = c_dk_drataframe['ind', 'attr'].distinct()

        return self.noisy_cells


    def get_noisy_cells_total(self):
        """
        Return a dataframe that consist of index of noisy cells index,attribute

        :return: spark_dataframe
        """
        self.get_noisy_cells_for_dc(self.all_dcs[0])
        self.holo_obj.logger.info('Denial Constraint Queries: ')

        table_name = self.dataset.table_specific_name("C_dk_temp")
        query_for_creation_table = "CREATE TABLE " + table_name + \
                                   "(dc_index INT, t1_ind INT," \
                                   " t2_ind INT);"
        self.dataengine.query(query_for_creation_table)
        self.final_dc = []
        dc_index = 0
        t1_dataframe = None
        t2_dataframe = None
        for dc_name in self.dictionary_dc:
            query = "INSERT INTO " + table_name + \
                    "(SELECT DISTINCT " + str(dc_index) + \
                    " as dc_index, t1.index as t1_ind, " \
                    "t2.index as t2_ind "  \
                    " FROM  " + \
                    self.dataset.table_specific_name("Init") + \
                    " as t1, " + \
                    self.dataset.table_specific_name("Init") + \
                    " as  t2 " + "WHERE t1.index != t2.index  AND " + dc_name\
                    + " )"
            if self.holo_obj.verbose:
                self.holo_obj.logger.info(
                    " Start execution of query: ")
                self.holo_obj.logger.info(query)
                print query
                self.holo_obj.logger.info("  ")

            t0 = time.time()
            self.dataengine.query(query)
            t1 = time.time()
            if self.holo_obj.verbose:
                self.holo_obj.logger.info(
                    " Query Execution time: " + str(t1 - t0))
                self.holo_obj.logger.info(str(query))
                self.holo_obj.logger.info("  ")

            t1_attributes = set()
            t2_attributes = set()

            dc_predicates = self.dictionary_dc[dc_name]
            for predicate_index in range(0, len(dc_predicates)):
                predicate_type = dc_predicates[predicate_index][4]
                # predicate_type 0 : we do not have a literal in this predicate
                # predicate_type 1 : literal on the left side of the predicate
                # predicate_type 2 : literal on the right side of the predicate
                if predicate_type == 0:
                    relax_indices = range(2, 4)
                elif predicate_type == 1:
                    relax_indices = range(3, 4)
                elif predicate_type == 2:
                    relax_indices = range(2, 3)
                else:
                    raise ValueError(
                        'predicate type can only be 0: '
                        'if the predicate does not have a literal'
                        '1: if the predicate has a literal in the left side,'
                        '2: if the predicate has a literal in right side'
                    )
                for relax_index in relax_indices:
                    name_attribute = \
                        dc_predicates[predicate_index][relax_index].split(".")
                    if name_attribute[0] == "t1":
                        t1_attributes.add(name_attribute[1])
                    elif name_attribute[0] == "t2":
                        t2_attributes.add(name_attribute[1])

            t1_attributes_list = []
            t2_attributes_list = []
            for attribute in t1_attributes:
                t1_attributes_list.append([dc_index, attribute])
            for attribute in t2_attributes:
                t2_attributes_list.append([dc_index, attribute])

            t1_attributes_dataframe = self.spark_session.createDataFrame(
                t1_attributes_list, StructType([
                    StructField("dc_index", IntegerType(), False),
                    StructField("attr_name", StringType(), False),
                ]))

            t2_attributes_dataframe = self.spark_session.createDataFrame(
                t2_attributes_list, StructType([
                    StructField("dc_index", IntegerType(), False),
                    StructField("attr_name", StringType(), False),
                ]))

            if t1_dataframe is not None:
                t1_dataframe = t1_dataframe.union(t1_attributes_dataframe)
            else:
                t1_dataframe = t1_attributes_dataframe

            if t2_dataframe is not None:
                t2_dataframe = t2_dataframe.union(t2_attributes_dataframe)
            else:
                t2_dataframe = t2_attributes_dataframe

            dc_index = dc_index + 1

        t1_name = self.dataset.table_specific_name("T1_attributes")
        t2_name = self.dataset.table_specific_name("T2_attributes")
        self.dataengine._dataframe_to_table(t1_name, t1_dataframe)
        self.dataengine._dataframe_to_table(t2_name, t2_dataframe)

        query = " SELECT t1_ind as ind, attr_name as attr " \
                "FROM " + \
                self.dataset.table_specific_name("C_dk_temp") +\
                " as dk_temp," +\
                self.dataset.table_specific_name("T1_attributes") +\
                " as t1_attr " \
                "where dk_temp.dc_index = t1_attr.dc_index" \
                " UNION " \
                " SELECT t2_ind as ind, attr_name as attr " \
                "FROM " + \
                self.dataset.table_specific_name("C_dk_temp") + \
                " as dk_temp," +\
                self.dataset.table_specific_name("T2_attributes") + \
                " as t2_attr " \
                " where dk_temp.dc_index = t2_attr.dc_index "

        if self.holo_obj.verbose:
            self.holo_obj.logger.info(
                " Start execution of query: ")
            self.holo_obj.logger.info(query)
            print query
            self.holo_obj.logger.info("  ")

        t0 = time.time()
        c_dk_dataframe = self.dataengine.query(query, 1)
        t1 = time.time()
        if self.holo_obj.verbose:
            self.holo_obj.logger.info(
                " Query Execution time: " + str(t1 - t0))
            self.holo_obj.logger.info(str(query))
            self.holo_obj.logger.info("  ")

        self.noisy_cells = c_dk_dataframe
        return c_dk_dataframe

    def get_clean_cells(self):
        """
        Return a dataframe that consist of index of clean cells index,attribute
        :param dataframe: spark dataframe
        :param noisy_cells: list of noisy cells
        :return:
        """
        all_attr = self.dataengine.get_schema(self.dataset, "Init").split(
            ',')
        all_attr.remove('index')
        number_of_tuples = \
            self.dataengine.query(
                "Select count(*) as size FROM "
                + self.dataset.table_specific_name("Init"),
                1).collect()[0].size

        tuples = [ [i] for i in range(1,number_of_tuples+1)]
        attr = [[a] for a in all_attr]
        tuples_dataframe = self.spark_session.createDataFrame(
            tuples, ['ind'])
        attr_dataframe = self.spark_session.createDataFrame(
            attr, ['attr'])

        c_clean_dataframe = tuples_dataframe.crossJoin(attr_dataframe).\
            subtract(self.noisy_cells)

        return c_clean_dataframe
