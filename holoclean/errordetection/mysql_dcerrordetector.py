from holoclean.utils.dcparser import DCParser


class Mysql_DCErrorDetection:
    """
    This class return error
    cells and clean
    cells based on the
    denial constraint
    """

    def __init__(self, DenialConstraints, holo_obj, dataset):
        """
        This constructor at first convert all denial constraints
        to the form of SQL constraints
        and it get dataengine to connect to the database
        :param DenialConstraints: list of denial constraints that use
        :param dataengine: a connector to database
        :param dataset: list of tables name
        :param spark_session: spark session configuration
        """
        self.and_of_preds, self.null_pred = \
            DCParser(DenialConstraints, holo_obj.dataengine, dataset).get_anded_string('all')
        self.dataengine = holo_obj.dataengine
        self.dataset = dataset
        self.spark_session = holo_obj.spark_session
        self.holo_obj = holo_obj

    # Private methods
    def _create_new_dc(self):
        """
        For each dc we change the predicates, and return the new type of dc
        """
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        self.final_dc = []
        for dc_part in self.and_of_preds:
            list_preds = self._find_predicates(dc_part)
            for predicate in list_preds:
                attribute = self._change_predicates_for_query(predicate, attributes)
                self.final_dc.append([attribute, dc_part])

        return

    def _change_predicates_for_query(self, pred, attributes):
        """
                For each predicates we change it to the form that we need for
                the query to create the featurization table
                Parameters
                --------
                list_preds: a list of all the predicates of a dc
                attributes: a list of attributes of our initial table
        """

        operationsarr = ['<>', '<=', '>=', '=', '<', '>']

        components_preds = pred.split('.')
        for components_index in (0, len(components_preds) - 1):
            if components_preds[components_index] in attributes:
                for operation in operationsarr:
                    if operation in components_preds[components_index - 1]:
                        attribute = components_preds[
                                    components_index]
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
        :param dataset: list od dataset names
        :return: spark_dataframe
        """
        self.holo_obj.logger.info('Denial Constraint Queries: ')
        self._create_new_dc()
        query_for_featurization = "CREATE TABLE " + self.dataset.table_specific_name("C_dk_temp") + \
                                  "(ind INT, attr VARCHAR(255));"
        self.dataengine.query(query_for_featurization)
        for dc in self.final_dc:
            query = " ( " \
               "SELECT DISTINCT " \
               "table1.index as ind, " \
               + "'" + dc[0] + "'" + " AS attr " \
               " FROM  " + \
               self.dataset.table_specific_name("Init") + " as table1, " + \
               self.dataset.table_specific_name("Init") + " as  table2 " + \
               "WHERE table1.index != table2.index  AND " + dc[1] + " )"

            insert_dk_query = "INSERT INTO " + self.dataset.table_specific_name("C_dk_temp") + query + ";"
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
        query_for_featurization = "CREATE TABLE " + self.dataset.table_specific_name("C_clean_temp") + \
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
            insert_dk_query = "INSERT INTO " + self.dataset.table_specific_name("C_clean_temp") + query + ";"
            self.dataengine.query(insert_dk_query)
        df = self.dataengine.get_table_to_dataframe('C_clean_temp', self.dataset)
        c_clean_dataframe = df.subtract(noisy_cells)

        return c_clean_dataframe
