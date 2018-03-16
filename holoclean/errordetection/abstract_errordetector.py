from abc import ABCMeta, abstractmethod


class Abstract_Error_Detection:
    """
    This class is an abstract class for general error_detection , it requires for every sub-class to implement the
    get_clean_cells and get_noisy_cells method
    """
    __metaclass__ = ABCMeta

    def __init__(self, holo_obj, dataset):
        """
        :param dataengine: a connector to database
        :param dataset: list of tables name
        """
        self.dataengine = holo_obj.dataengine
        self.dataset = dataset
        self.spark_session = holo_obj.spark_session
        self.holo_obj = holo_obj

    @abstractmethod
    def get_noisy_cells(self):
        """
         This method creates a dataframe which has the informations (index,attribute) for the dk_cells

        :return dataframe  for the dk_cell
        """
        pass

    @abstractmethod
    def get_clean_cells(self):
        """
         This method creates a dataframe which has the informations (index,attribute) for the clean_cells

        :return dataframe  for the clean_cells

        """
        pass
