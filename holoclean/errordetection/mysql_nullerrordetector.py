from errordetector import ErrorDetection
from holoclean.global_variables import GlobalVariables
import time


__metaclass__ = type


class MysqlnullErrorDetection(ErrorDetection):
    """
    This class is a subclass of the errordetector class and
    will return  error  cells and clean cells based on if they have null value
    """

    def __init__(self, session):
        """


        :param session: Holoclean session
        """
        super(MysqlnullErrorDetection, self).\
            __init__(session.holo_env, session.dataset)
        self.index = GlobalVariables.index_name

        self.session = session

    def get_noisy_cells(self):
        """
        Return a dataframe that consist of index of noisy cells index,attribute

        :return: spark_dataframe
        """

        table_name = self.dataset.table_specific_name("C_dk_temp_null")
        query_for_creation_table = "CREATE TABLE " + table_name + \
                                   "(ind INT, attr VARCHAR(255));"
        self.dataengine.query(query_for_creation_table)
        self.discovering_cells_with_null_values()
        self.noisy_cells = self.dataengine.\
            get_table_to_dataframe("C_dk_temp_null", self.dataset)

        return self.noisy_cells

    def discovering_cells_with_null_values(self):
        """
        Adds to C_dk_temp_null table all the cells that are NULL
        """

        all_attr = self.session.dataset.get_schema('Init')
        all_attr.remove(self.index)
        for attribute in all_attr:
            time_start = time.time()
            t_name = self.dataset.table_specific_name("Init")
            query_null = "INSERT INTO " + \
                         self.dataset.table_specific_name("C_dk_temp_null") + \
                         " SELECT t1.__ind as ind,'" + attribute +\
                         "' as attr  " \
                         "FROM " + t_name + " AS t1 " \
                         "WHERE t1." + attribute + " is NULL"
            self.dataengine.query(query_null)
            time_end = time.time()
            if self.holo_obj.verbose:
                self.holo_obj.logger.info("Time for executing query "
                                          + query_null + ":" + str(time_end - time_start))

    def get_clean_cells(self):
        """
        Return a dataframe that consist of index of clean cells index,attribute
        :return:
        """
        c_clean_dataframe = self.session.init_flat.\
            subtract(self.noisy_cells)
        return c_clean_dataframe
