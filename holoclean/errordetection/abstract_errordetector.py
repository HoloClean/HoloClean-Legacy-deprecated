from abc import ABCMeta, abstractmethod

class Abstract_Error_Detection:
    """
    This class is an abstract class for general error_de, it requires for every sub-class to implement the
    get_query method
    """
    __metaclass__ = ABCMeta

    def __init__(self, dataengine, dataset):
        """
        :param dataengine: a connector to database
        :param dataset: list of tables name
        """
        self.dataengine = dataengine
        self.dataset = dataset


    @abstractmethod
    def get_query(self):
        """
         This method should return a string or strings of the query/queries that
         are used to create the Signal

        """
        pass
