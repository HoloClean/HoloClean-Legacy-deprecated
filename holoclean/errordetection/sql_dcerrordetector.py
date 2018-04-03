from errordetector import ErrorDetection
from holoclean.global_variables import GlobalVariables
from holoclean.utils.parser_interface import DenialConstraint
import time


__metaclass__ = type


class SqlDCErrorDetection(ErrorDetection):
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
        super(SqlDCErrorDetection, self).\
            __init__(session.holo_env, session.dataset)
        self.session = session
        self.index = GlobalVariables.index_name
        self.dc_parser = session.parser
        self.operationsarr = DenialConstraint.operationsArr
        self.noisy_cells = None
        self.dc_objects = session.dc_objects
        self.Denial_constraints = session.Denial_constraints

    # Internals
    @staticmethod
    def _is_symmetric( dc_name):
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
        dc_object = self.dc_objects[dc_name]
        temp_table = "tmp" + self.dataset.dataset_id

        # Create Query for temp table
        query = "CREATE TABLE " + temp_table +\
                " AS SELECT "
        for tuple_name in dc_object.tuple_names:
            query += tuple_name + "." + self.index + " as " + \
                     tuple_name + "_ind,"

        query = query[:-1]
        query += " FROM  "
        for tuple_name in dc_object.tuple_names:
            query += self.dataset.table_specific_name("Init") + \
                 " as " + tuple_name + ","
        query = query[:-1]
        query += " WHERE "
        if len(dc_object.tuple_names) == 2:
            query += dc_object.tuple_names[0] + "." + self.index + \
                 " != " + dc_object.tuple_names[1] + "." + self.index + " AND "
        query += dc_object.cnf_form
        self.dataengine.query(query)

        t4 = time.time()
        if self.holo_obj.verbose:
            self.holo_obj.logger.info("Time for executing query "
                                      + dc_name + ":" + str(t4-t3))

        # For each predicate add attributes
        tuple_attributes = {}
        for tuple_name in dc_object.tuple_names:
            tuple_attributes[tuple_name] = set()

        for predicate in dc_object.predicates:

            for component in predicate.components:
                if isinstance(component, str):
                    pass
                else:
                    tuple_attributes[component[0]].add(component[1])

        tuple_attributes_lists ={}
        tuple_attributes_dfs = {}
        for tuple_name in dc_object.tuple_names:
            tuple_attributes_lists[tuple_name] = [[i] for i in
                                                  tuple_attributes[tuple_name]]
            tuple_attributes_dfs[
                tuple_name] = self.spark_session.createDataFrame(
                tuple_attributes_lists[tuple_name], ['attr_name'])

            name = self.dataset.table_specific_name(tuple_name + "_attributes")
            attribute_dataframe = tuple_attributes_dfs[tuple_name]

            self.dataengine.dataframe_to_table(name, attribute_dataframe)

            distinct = \
                "(SELECT DISTINCT " + tuple_name + "_ind " \
                                                   " FROM " + \
                temp_table + ") AS row_table"

            query = "INSERT INTO " + \
                    self.dataset.table_specific_name("C_dk_temp") + \
                    " SELECT row_table. " + tuple_name + "_ind as ind," \
                    " a.attr_name as attr FROM " + \
                    name + \
                    " AS a," + \
                    distinct
            self.dataengine.query(query)
            self.holo_obj.logger.info('Denial Constraint Query Left ' +
                                      dc_name + ":" + query)
            drop_temp_table = "DROP TABLE " + name
            self.dataengine.query(drop_temp_table)
        drop_temp_table = "DROP TABLE " + temp_table
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
        self.dataengine.dataframe_to_table(t1_name, t1_attributes_dataframe)

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

    def get_noisy_cells(self):
        """
        Returns a dataframe that consist of index of noisy cells index,attribute

        :return: spark_dataframe
        """

        table_name = self.dataset.table_specific_name("C_dk_temp")
        query_for_creation_table = "CREATE TABLE " + table_name + \
                                   "(ind INT, attr VARCHAR(255));"
        self.dataengine.query(query_for_creation_table)
        for dc_name in self.dc_objects:
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
