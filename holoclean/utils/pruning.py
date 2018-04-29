from abc import ABCMeta, abstractmethod
from holoclean.global_variables import GlobalVariables


class RandomVar:
    """RandomVar class: class for random variable"""

    def __init__(self, **kwargs):
        """
        Initializing random variable objects
        :param kwargs: dictionary of properties
        """
        self.__dict__.update(kwargs)


class Pruning:
    """
    This class is an abstract class for general pruning, it requires for
    every sub-class to implement the create_dataframe method
    """
    __metaclass__ = ABCMeta

    def __init__(self, session):
        """
        Initializing Featurizer object abstraction
        :param session: session object
        """
        self.session = session
        self.spark_session = session.holo_env.spark_session
        self.dataengine = session.holo_env.dataengine
        self.cellvalues = self._c_values()
        self.dirty_cells_attributes = set([])
        self._d_cell()
        self.cell_domain = {}

    def _c_values(self):
        """
        Create cell value dictionary from the init table
        :return: dictionary of cells values
        """
        dataframe_init = self.session.init_dataset
        table_attribute = dataframe_init.columns
        row_id = 0
        cell_values = {}
        self.attribute_map = {}
        number_id = 0

        for record in dataframe_init.drop(
                GlobalVariables.index_name).collect():
            # for each record creates a new row
            row = {}
            column_id = 0
            for column_value in record:
                # For each column
                self.attribute_map[table_attribute[column_id]] = column_id
                cell_variable = RandomVar(
                    columnname=table_attribute[column_id],
                    value=column_value, tupleid=row_id,
                    cellid=number_id, dirty=0, domain=0)
                row[column_id] = cell_variable
                number_id = number_id + 1
                column_id = column_id + 1
            cell_values[row_id] = row
            row_id = row_id + 1
        return cell_values

    def _d_cell(self):
        """
        Finds the attributes that appear in don't know cells and flags the cells
        in cellvalues that are dirty
        :return: None
        """
        dataframe_dont_know = self.session.dk_df
        for cell in dataframe_dont_know.collect():

            # This part gets the attributes of noisy cells
            self.dirty_cells_attributes.add(cell[1])

            self.cellvalues[int(cell[0]) - 1][
                self.attribute_map[cell[1]]].dirty = 1

        return

    @abstractmethod
    def get_domain(self):
        """
         This method is used to populate the cell_domain to get the possible
         values for each cell
        """
        pass

    @abstractmethod
    def _create_dataframe(self):
        """
        Creates spark dataframes from cell_domain for all the cells, then creates
        the possible values tables and kij_lookup for the domain in sql
        """
    pass