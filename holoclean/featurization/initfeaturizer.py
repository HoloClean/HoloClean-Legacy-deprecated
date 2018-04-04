from featurizer import Featurizer


__metaclass__ = type


class SignalInit(Featurizer):
    """
    This class is a subclass of Featurizer class and
    will return the  query which represent the Initial Signal for the
    clean and don't know cells
    """

    def __init__(self, session):
        """
        Constructing initial values signal object

        :param session: session object
        """
        super(SignalInit, self).__init__(session)
        self.id = "SignalInit"
        self.table_name = self.dataset.table_specific_name('Init')
        self.count = 1

    def get_query(self, clean=1):
        """
        Creates a string for the query that it is used to create the Initial
        Signal

        :param clean: shows if create the feature table for the clean or
        the don't know cells

        :return a list of length 1 with a string with the query
        for this feature
        """
        if clean:
            self.offset = self.session.feature_count
        count = self.offset + 1
        if clean:
            name = "Observed_Possible_values_clean"
        else:
            name = "Observed_Possible_values_dk"

        query_for_featurization = " SELECT  \
            init_flat.vid as vid, init_flat.domain_id AS assigned_val, \
            '" + str(count) + "' AS feature, \
            1 as count\
            FROM """ + \
            self.dataset.table_specific_name(name) + \
            " AS init_flat WHERE vid IS NOT NULL"

        # if clean add signal fo Feature_id_map
        if clean:
            self.session.feature_count += count

            index = self.offset + count
            list_domain_map = [[index, 'Init', 'Init', 'Init']]
            df_domain_map = self.session.holo_env.spark_session.\
                createDataFrame(list_domain_map,
                                self.dataset.attributes['Feature_id_map'])
            self.session.holo_env.dataengine.add_db_table(
                'Feature_id_map', df_domain_map, self.dataset, append=1)
        return [query_for_featurization]
