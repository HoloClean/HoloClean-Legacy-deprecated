from abc import ABCMeta, abstractmethod


class Featurizer:
    """
    This class is an abstract class for general featurizer, it requires for
    every sub-class to implement the
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
         This method creates a string or strings of the query/queries that are
         used to create the Signal

        :return a string or a list of strings of the query/queries that
         are used to create the Signal
        """
        pass
