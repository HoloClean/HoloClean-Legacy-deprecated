from holoclean.utils.dcparser import DCParser
from errordetector import ErrorDetection

__metaclass__ = type


class MysqlDCErrorDetection(ErrorDetection):
    """
    This class is a subclass of the errordetector class and
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
        dc_parser = DCParser(DenialConstraints)
        self.and_of_preds = dc_parser.get_anded_string('all')
        self.operationsarr = DCParser.operationsArr
        self.table_names = DCParser.tables_name
        self.noisy_cells = None

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
                    if component.find("t1.") == -1 and \
                            component.find("t2.") == -1:
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
        This method finds the predicates of dc

        :param cond: a denial constraint
        :return: list_preds: list of predicates
        """

        list_preds = cond.split(' AND ')
        return list_preds

    # Setters

    # Getters

    def get_noisy_cells(self):
        """
        Return a dataframe that consist of index of noisy cells index,attribute

        :return: spark_dataframe
        """
        self.holo_obj.logger.info('Denial Constraint Queries: ')
        self._create_new_dc()
        dataframe = None
        for dc in self.final_dc:
            for table in self.table_names:
                query = " ( " \
                        "SELECT DISTINCT " + \
                        table + ".index as ind, " \
                        + "'" + dc[0] + "'" + " AS attr " \
                        " FROM  " + \
                        self.dataset.table_specific_name("Init") + \
                        " as t1, " + \
                        self.dataset.table_specific_name("Init") +\
                        " as  t2 " + \
                        "WHERE t1.index != t2.index  AND " \
                        + dc[1] + " )"
                if dataframe is not None:
                    dataframe = dataframe.union(
                        self.dataengine.query(query, spark_flag=1))
                else:
                    dataframe = self.dataengine.query(query, spark_flag=1)

        c_dk_dataframe = dataframe.distinct()
        self.noisy_cells = c_dk_dataframe
        return c_dk_dataframe

    def get_clean_cells(self):
        """
        Return a dataframe that consist of index of clean cells index,attribute

        :param dataframe: spark dataframe
        :param noisy_cells: list of noisy cells
        :return:
        """
        noisy_cells = self.noisy_cells
        all_attr = self.dataengine.get_schema(self.dataset, "Init").split(',')
        all_attr.remove('index')
        dataframe = None
        for attribute in all_attr:
            query = " ( " \
                "SELECT  " \
                "t1.index as ind, " \
                + "'" + attribute + "'" + " AS attr " \
                " FROM  " + \
                self.dataset.table_specific_name("Init") + " as t1 )"
            if dataframe is not None:
                dataframe = dataframe.union(
                    self.dataengine.query(query, spark_flag=1))
            else:
                dataframe = self.dataengine.query(query, spark_flag=1)

        if dataframe:
            c_clean_dataframe = dataframe.subtract(noisy_cells)
        else:
            c_clean_dataframe = None
        return c_clean_dataframe
