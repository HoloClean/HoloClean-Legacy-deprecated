from holoclean.utils.dcparser import DCParser
from errordetector import ErrorDetection

__metaclass__ = type


class MysqlDCErrorDetection(ErrorDetection):
    """
    This class is a subclass of the abstract_errodetector class and
    will return  error  cells and clean cells based on the
    denial constraint
    """

    def __init__(self, DenialConstraints, holo_obj, dataset):
        """
        This constructor  converts all denial constraints
        to the form of SQL constraints

        :param DenialConstraints: list of denial constraints that use
        :param holo_obj: a holoclean object
        :param dataset: list of tables name
        """
        super(MysqlDCErrorDetection, self).__init__(holo_obj, dataset)
        self.and_of_preds = DCParser(
            DenialConstraints)\
            .get_anded_string('all')
        self.operationsarr = ['=', '<>', '<=', '>=', '<', '>']

    # Private methods
    def _create_new_dc(self):
        """
        For each dc we change the predicates, and return the new type of dc

        :return:
        """
        self.final_dc = []
        for dc_part in self.and_of_preds:
            list_preds = self._find_predicates(dc_part)
            for predicate in list_preds:
                attribute = self._find_predicate(predicate)
                self.final_dc.append([attribute, dc_part])

        return

    def _find_predicate(self, predicate):
        """
         This method returns the attribute of each predicate

         :param predicate: the predicate
         :return: list_preds: a list of all the predicates of a dc
         :return: attributes: a list of attributes of our initial table
        """

        for operation in self.operationsarr:
            if operation in predicate:
                components = predicate.split(operation)
                for component in components:
                    if component.find("table1.") == -1 and \
                            component.find("table2.") == -1:
                        pass
                    else:
                        attributes = component.split(".")
                        attribute = attributes[1]
                        break
                break

        return attribute

    @staticmethod
    def _find_predicates(cond):
        """
        This method finds the predicates of dc"

        :param cond: a denial constrain
        :rtype: list_preds: list of predicates
        """

        list_preds = cond.split(' AND ')
        return list_preds

    # Setters

    # Getters

    def get_noisy_cells(self, dataset):
        """
        Return a dataframe that consist of index of noisy cells index,attribute

        :param dataset: list of dataset names
        :return: spark_dataframe
        """
        self.holo_obj.logger.info('Denial Constraint Queries: ')
        self._create_new_dc()
        query_for_featurization = "CREATE TABLE " + \
            self.dataset.table_specific_name("C_dk_temp") +\
            "(ind INT, attr VARCHAR(255));"
        self.dataengine.query(query_for_featurization)
        for dc in self.final_dc:
            tables = ["table1", "table2"]
            for table in tables:
                query = " ( " \
                        "SELECT DISTINCT " + \
                        table + ".index as ind, " \
                        + "'" + dc[0] + "'" + " AS attr " \
                        " FROM  " + \
                        self.dataset.table_specific_name("Init") + \
                        " as table1, " + \
                        self.dataset.table_specific_name("Init") +\
                        " as  table2 " + \
                        "WHERE table1.index != table2.index  AND " \
                        + dc[1] + " )"
                insert_dk_query = "INSERT INTO " + \
                                  self.dataset.table_specific_name("C_dk_temp")\
                                  + query + ";"
                self.dataengine.query(insert_dk_query)
        df = self.dataengine.get_table_to_dataframe('C_dk_temp', self.dataset)
        c_dk_dataframe = df.distinct()

        return c_dk_dataframe

    def get_clean_cells(self, dataframe, noisy_cells):
        """
        Return a dataframe that consist of index of clean cells index,attribute

        :param dataframe: spark dataframe
        :param noisy_cells: list of noisy cells
        :return:
        """
        query_for_featurization = "CREATE TABLE " + \
            self.dataset.table_specific_name("C_clean_temp") +\
            "(ind INT, attr VARCHAR(255));"
        self.dataengine.query(query_for_featurization)
        all_attr = self.dataengine.get_schema(self.dataset, "Init").split(',')
        all_attr.remove('index')

        for attribute in all_attr:
            query = " ( " \
                "SELECT  " \
                "table1.index as ind, " \
                + "'" + attribute + "'" + " AS attr " \
                " FROM  " + \
                self.dataset.table_specific_name("Init") + " as table1 )"
            insert_dk_query = "INSERT INTO " + \
                self.dataset.table_specific_name("C_clean_temp") + query + ";"
            self.dataengine.query(insert_dk_query)
        df = self.dataengine.get_table_to_dataframe(
            'C_clean_temp', self.dataset)
        c_clean_dataframe = df.subtract(noisy_cells)

        return c_clean_dataframe
