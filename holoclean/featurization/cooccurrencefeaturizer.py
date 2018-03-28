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

        self.count = 0
        self.pruning_object = self.session.pruning
        self.domain_pair_stats = self.pruning_object.domain_pair_stats
        self.dirty_cells_attributes = \
            self.pruning_object.dirty_cells_attributes
        self.domain_stats = self.pruning_object.domain_stats
        self.threshold = self.pruning_object.threshold
        self.cell_values_init = self.pruning_object.cell_values_init

    def _create_cooccur_dataframe(self, dataframe):

        cooccur_list = []
        for row in dataframe:
            vid = row[0]
            tid = row[1]
            attr_name = row[2]
            attr_val = row[3]
            domain_id = row[5]
            for attribute in self.dirty_cells_attributes:
                if attribute != attr_name:
                    cooccur_value = self.cell_values_init[tid-1][attribute]

                    if (attr_val, cooccur_value) in self.domain_pair_stats[attr_name][attribute]:
                        c_cooccur_counts = self.domain_pair_stats[attr_name][attribute][
                                (attr_val, cooccur_value)]
                        v_cooccur_counts = self.domain_stats[attribute][cooccur_value]
                        cooccur_prob = int(c_cooccur_counts) / v_cooccur_counts
                        if cooccur_prob > self.threshold :
                            cooccur_count = 1
                        else:
                            cooccur_count = 0
                    else:
                        cooccur_count = 0
                    cooccur_list.append(
                        [vid, domain_id, self.attribute_feature_id[attribute],
                         cooccur_count])
        return cooccur_list

    def _create_cooccur_feature_table(self, clean):
        """

        :param clean: flag if we are in the training or testing phase
        """
        if clean:
            table_name = 'Feature_cooccur_clean'
            self.offset = self.session.feature_count
            self.attribute_feature_id = {}
            feature_id_list = []
            for attribute in self.dirty_cells_attributes:
                self.count += 1
                self.attribute_feature_id[attribute] = self.count + self.offset
                feature_id_list.append\
                    ([self.count + self.offset, attribute, 'Cooccur', 'Cooccur'])
            feature_df = self.session.holo_env.spark_session.createDataFrame(
                feature_id_list,
                self.session.dataset.attributes['Feature_id_map']
            )
            self.dataengine.add_db_table(
                'Feature_id_map',
                feature_df,
                self.session.dataset,
                append=1
            )
            dataframe = self.session.possible_values_clean
            self.session.feature_count += self.count

        else:
            table_name = 'Feature_cooccur_dk'
            dataframe = self.session.possible_values_dk

        cooccur_list = self._create_cooccur_dataframe(dataframe)

        self.feature_list = cooccur_list
        # new_df_cooccur = self.session.holo_env.spark_session.createDataFrame(
        #     cooccur, self.dataset.attributes['Feature']
        # )
        #
        # self.dataengine.add_db_table(table_name, new_df_cooccur, self.dataset)

        return

    def get_query(self, clean=1):
        """
        Creates a string for the query that it is used to create the  Cooccur
        Signal

        :param clean: shows if create the feature table for the clean or the dk
         cells

        :return a string with the query for this feature
        """
        self._create_cooccur_feature_table(clean)
        # Create co-occur feature
        # if clean:
        #     table_name = "Feature_cooccur_clean"
        # else:
        #     table_name = "Feature_cooccur_dk"
        # query_for_featurization = "SELECT * FROM " + \
        #                           self.session.dataset.table_specific_name(table_name)

        return []
