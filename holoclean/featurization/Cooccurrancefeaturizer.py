from featurizer import Featurizer


class SignalCooccur(Featurizer):
    """
    This class is a subclass of the Featurizer class and
    will return the mysql query which represent the Initial Signal for the clean and dk cells
    """

    def __init__(self, attr_constrained, dataengine, dataset):

        """

        :param attr_constrained: list of atttributes that are part of a dc
        :param dataengine: a connector to database
        :param dataset: list of tables name
        """

        super.__init__(dataengine, dataset)
        self.id = "SignalCooccur"
        self.attr_constrained = attr_constrained
        self.table_name = self.dataset.table_specific_name('Init')

    def _get_constraint_attibute(self, table_name, attr_column_name):
        """
        Creates a string with a condition for only checking the attributes that are part of a DC violation

        :param  table_name: the name of the table that we need to check the attributes
        :param  attr_column_name: the name of the columns of table that we want to enforce the condition

        :return a string with the condition
        """

        specific_features = "("

        for const in self.attr_costrained:
            specific_features += table_name + "." + attr_column_name + " = '" \
                                 + const + "' OR "
        specific_features = specific_features[:-4]
        specific_features += ")"
        return specific_features


    def get_query(self, clean=1):
        """
        Creates a string for the query that it is used to create the Initial Signal

        :param clean: shows if create the feature table for the clean or the dk cells

        :return a string with the query for this feature
        """
        if clean:
            name = "Observed_Possible_values_clean"
            init_flat = "Init_flat_join"
            c = "C_clean_flat"
        else:
            name = "Observed_Possible_values_dk"
            init_flat = "Init_flat_join_dk"
            c = "C_dk_flat"

        query_init_flat_join = "CREATE TABLE " + \
                               self.dataset.table_specific_name(init_flat) + \
                               " ( " \
                               "SELECT DISTINCT " \
                               "t1.vid as vid_first, " \
                               "t1.tid AS tid_first, " \
                               "t1.attr_name AS attr_first, " \
                               "t1.domain_id AS val_first," \
                               "t2.tid AS tid_second, " \
                               "t2.attribute AS attr_second, " \
                               "t2.value AS val_second, " \
                               "t3.feature_ind as feature_ind " \
                               "FROM " + \
                               self.dataset.table_specific_name(name) +\
                               " t1, " + \
                               self.dataset.\
                               table_specific_name(c) + " t2, " + \
                               self.dataset.\
                               table_specific_name('Feature_id_map') + " t3 " \
                               "WHERE t1.tid = t2.tid AND " \
                               "t1.attr_name != t2.attribute AND " \
                               " t3.attribute=t2.attribute AND " \
                               " t3.value=t2.value) ;"
        self.dataengine.query(query_init_flat_join)

        # Create co-occur feature

        query_for_featurization = " (SELECT DISTINCT " \
                                  "cooccur.vid_first as vid, " \
                                  "cooccur.val_first AS assigned_val, " \
                                  " feature_ind AS feature, " \
                                  " 1 as count " \
                                  "FROM " \
                                  + self.dataset.\
                                  table_specific_name(init_flat) + \
                                  " AS cooccur  " +\
                                  "WHERE " + \
                                  self.get_constraint_attibute('cooccur',
                                                               'attr_first')
        return query_for_featurization