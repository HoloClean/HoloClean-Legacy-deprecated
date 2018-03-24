from featurizer import Featurizer


__metaclass__ = type


class SignalInit(Featurizer):
    """
    This class is a subclass of the Featurizer class and
    will return the mysql query which represent the Initial Signal for the
    clean and dk cells
    """

    def __init__(self, dataengine, dataset):

        """

        :param attr_constrained: list of atttributes that are part of a dc
        :param dataengine: a connector to database
        :param dataset: list of tables name
        """

        super(SignalInit, self).__init__(dataengine, dataset)
        self.id = "SignalInit"
        self.table_name = self.dataset.table_specific_name('Init')

    def get_query(self, clean=1):
        """
        Creates a string for the query that it is used to create the Initial
        Signal

        :param clean: shows if create the feature table for the clean or the dk
         cells

        :return a list of length 1 with string with the query for this feature
        """
        if clean:
            name = "Observed_Possible_values_clean"
        else:
            name = "Observed_Possible_values_dk"

        query_for_featurization = """ SELECT  \
            init_flat.vid as vid, init_flat.domain_id AS assigned_val, \
            '1' AS feature, \
            1 as count\
            FROM """ + \
            self.dataset.table_specific_name(name) + \
            " AS init_flat "
        return [query_for_featurization]
