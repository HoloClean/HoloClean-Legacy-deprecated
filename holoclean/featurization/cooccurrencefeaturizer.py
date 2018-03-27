from featurizer import Featurizer
from holoclean.global_variables import GlobalVariables



__metaclass__ = type


class SignalCooccur(Featurizer):
    """
    This class is a subclass of the Featurizer class and
    will return the mysql query which represent the  Cooccur signal for
     the clean and dk cells
    """

    def __init__(self, session):

        """

        :param attr_constrained: list of attributes that are part of a dc
        :param dataengine: a connector to database
        :param dataset: list of tables name
        """

        super(SignalCooccur, self).__init__(session)
        self.id = "SignalCooccur"
        self.offset = self.session.feature_count
        self.index_name = GlobalVariables.index_name
        self.all_attr = list(self.session.init_dataset.schema.names)
        self.all_attr.remove(self.index_name)

        self.count = len(self.all_attr)
        self.pruning_object = self.session.pruning
        self.domain_pair_stats = self.pruning_object.domain_pair_stats
        self.dirty_cells_attributes = \
            self.pruning_object.dirty_cells_attributes
        self._create_cooccur_feature_table()

    def _create_cooccur_dataframe(self, dataframe ):

        cooccur_list = []
        for row in dataframe.collect():
            vid = row[0]
            tid = row[1]
            attr_name = row[2]
            attr_val = row[3]
            domain_id = row[5]
            for attribute in self.dirty_cells_attributes :
                if attribute != attr_name:
                    cooccur_value = 0
                    try:
                        cooccur_count = \
                            self.domain_pair_stats[attr_name][attribute][
                                (attr_val, cooccur_value)]
                    except:
                        cooccur_count = 0
                    cooccur_list.append(
                        [vid, domain_id, self.attribute_feature_id[attribute],
                         cooccur_count])
        return cooccur_list

    def _create_cooccur_feature_table(self):
        """

        :param clean: flag if we are in the training or testing phase
        """
        self.offset = self.session.feature_count
        self.attribute_feature_id = {}
        self.count = self.offset
        for attribute in self.dirty_cells_attributes:
            self.count = self.count + 1
            self.attribute_feature_id[attribute] = self.count

        cooccur_clean = self._create_cooccur_dataframe(
            self.session.possible_values_clean)
        cooccur_dk = self._create_cooccur_dataframe(
            self.session.possible_values_dk)

        new_df_cooccur_clean = self.session.holo_env.spark_session.createDataFrame(
            cooccur_clean, self.dataset.attributes['Feature']
        )

        new_df_cooccur_dk = self.session.holo_env.spark_session.createDataFrame(
            cooccur_dk, self.dataset.attributes['Feature']
        )

        self.dataengine.add_db_table('Feature_cooccur_clean',
                                     new_df_cooccur_clean, self.dataset)

        self.dataengine.add_db_table('Feature_cooccur_dk',
                                     new_df_cooccur_dk, self.dataset)
        return

    def get_query(self, clean=1):
        """
        Creates a string for the query that it is used to create the  Cooccur
        Signal

        :param clean: shows if create the feature table for the clean or the dk
         cells

        :return a string with the query for this feature
        """

        # Create co-occur feature
        query_for_featurization = " "

        return [query_for_featurization]
