from abc import ABCMeta, abstractmethod


class Featurizer:
    """
    This class is an abstract class for general featurizer, it requires for
    every sub-class to implement the get_query method
    """
    __metaclass__ = ABCMeta

    def __init__(self, session):
        """
        Initializing Featurizer object abstraction

        :param session: session object
        """
        self.session = session
        self.dataengine = session.holo_env.dataengine
        self.dataset = session.dataset

        # if the Signal creates a dataframe instead of using SQL
        self.direct_insert = False

        # These values must be overridden in subclass
        self.offset = 0  # offset on the feature_id_map
        self.id = 'Base'
        self.count = 0

    @abstractmethod
    def get_query(self):
        """
         This method creates a string or strings of the query/queries that are
         used to create the Signal

        :return a string or a list of strings of the query/queries that
         are used to create the Signal
        """
        pass
