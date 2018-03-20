from featurizer import Featurizer


__metaclass__ = type


class SignalInit(Featurizer):
    """
    This class is a subclass of the Featurizer class and
    will return the mysql query which represent the Initial Signal for the
    clean and dk cells
    """

    def __init__(self, attr_constrained, dataengine, dataset):

        """

        :param attr_constrained: list of atttributes that are part of a dc
        :param dataengine: a connector to database
        :param dataset: list of tables name
        """

        super(SignalInit, self).__init__(dataengine, dataset)
        self.id = "SignalInit"
        self.attr_constrained = attr_constrained
        self.table_name = self.dataset.table_specific_name('Init')

    def _get_constraint_attribute(self, table_name, attr_column_name):
        """
        Creates a string with a condition for only checking the attributes that
        are part of a DC

        :param  table_name: the name of the table that we need to check
        the attributes
        :param  attr_column_name: the name of the columns of table that we
        want to enforce the condition

        :return a string with the condition
        """

        specific_features = ""
        for const in self.attr_constrained:
            specific_features += table_name + "." + attr_column_name + " = '" \
                                 + const + "' OR "
        specific_features = specific_features[:-4]
        specific_features = "(" + specific_features + ")"
        return specific_features

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
            " AS init_flat " + \
            "WHERE " + self._get_constraint_attribute('init_flat', 'attr_name')
        return [query_for_featurization]
