from holoclean.utils.dcparser import DCParser
from errordetector import ErrorDetection
from holoclean.global_variables import GlobalVariables
import time


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
        super(MysqlDCErrorDetection, self).\
            __init__(session.holo_env, session.dataset)
        self.session = session
        self.index = GlobalVariables.index_name
        self.dc_parser = session.parser
        self.all_dcs = \
            self.dc_parser.get_CNF_of_dcs(session.Denial_constraints)
        self.operationsarr = DCParser.operationsArr
        self.noisy_cells = None
        self.dictionary_dc = self.dc_parser.create_dc_map(self.all_dcs)
        self.Denial_constraints = session.Denial_constraints

    # Internals

    def _is_symmetric(self, dc_name):
        result = True
        non_sym_ops = ['<=', '>=', '<', '>']
        for op in non_sym_ops:
            if op in dc_name:
                result = False
        return result

    def _get_noisy_cells_for_dc(self, dc_name):
        """
                Return a dataframe that consist of index of noisy cells index,
                attribute

                :param dc_name: String
                :return: spark_dataframe
                """

        if self.holo_obj.verbose:
            self.holo_obj.logger.info(
                'Denial Constraint Queries For ' + dc_name)
        t3 = time.time()
        temp_table = "tmp" + self.dataset.dataset_id
        query = "CREATE TABLE " + temp_table +\
                " AS SELECT " \
                "t1." + self.index + \
                " as t1_ind, " \
                "t2." + self.index + " as t2_ind " \
                " FROM  " + \
                self.dataset.table_specific_name("Init") + \
                " as t1, " + \
                self.dataset.table_specific_name("Init") + \
                " as  t2 " + "WHERE t1." + self.index + \
                " != t2." + self.index + "  AND " + dc_name
        self.dataengine.query(query)
        t4 = time.time()
        if self.holo_obj.verbose:
            self.holo_obj.logger.info("Time for executing query "
                                      + dc_name + ":" + str(t4-t3))

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
                     " SELECT row_table.t1_ind as ind," \
                     " a.attr_name as attr FROM " + \
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

    def _get_sym_noisy_cells_for_dc(self, dc_name):
        """
                Return a dataframe that consist of index of noisy cells index,
                attribute

                :param dc_name: String
                :return: spark_dataframe
                """

        self.holo_obj.logger.info('Denial Constraint Queries For ' + dc_name)
        temp_table = "tmp" + self.dataset.dataset_id
        query = "CREATE TABLE " + \
                temp_table + " AS SELECT " \
                             "t1." + self.index + \
                " as t1_ind, " \
                "t2." + self.index + \
                " as t2_ind " \
                " FROM  " + \
                self.dataset.table_specific_name("Init") + \
                " as t1, " + \
                self.dataset.table_specific_name("Init") + \
                " as  t2 " + "WHERE t1." + self.index + \
                " != t2." + self.index + \
                "  AND " + dc_name
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
                     " SELECT row_table.t1_ind as ind," \
                     " a.attr_name as attr FROM " + \
                     t1_name + \
                     " AS a," + \
                     distinct_left
        self.dataengine.query(query_left)

        self.holo_obj.logger.info('Denial Constraint Query Left ' +
                                  dc_name + ":" + query_left)

        drop_temp_table = "DROP TABLE " + temp_table
        self.dataengine.query(drop_temp_table)

    # Getters

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
            self._get_noisy_cells_for_dc(dc_name)

        c_dk_drataframe = self.dataengine.\
            get_table_to_dataframe("C_dk_temp", self.dataset)
        self.noisy_cells = c_dk_drataframe['ind', 'attr'].distinct()
        return self.noisy_cells



    def get_clean_cells(self):
        """
        Return a dataframe that consist of index of clean cells index,attribute
        :return:
        """
        c_clean_dataframe = self.session.init_flat.\
            subtract(self.noisy_cells)
        return c_clean_dataframe
