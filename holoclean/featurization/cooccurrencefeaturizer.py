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
        self.threshold = self.pruning_object.threshold1
        self.cell_values_init = self.pruning_object.cell_values_init
        self.direct_insert = True

    def insert_to_tensor(self, tensor, clean):
        variables, features, domain_size = tensor.size()
        cooccurences = self.pruning_object.coocurence_for_first_attribute
        if clean:
            vid_list = self.pruning_object.v_id_clean_list
            domain = self.pruning_object.domain_clean
        else:
            vid_list = self.pruning_object.v_id_dk_list
            domain = self.pruning_object.domain_dk
        for entry in domain:
            for f in range(self.offset, self.offset + self.count):
                vid = entry[0] - 1
                attribute = vid_list[vid][1]
                value = domain[entry]
                co_attribute = self.attribute_feature_id[f + 1]
                co_value = self.cell_values_init[vid_list[vid][0] - 1][co_attribute]
                try:
                    c = cooccurences[co_attribute][co_value][attribute][value]
                    tensor[vid, f, entry[1] - 1] = c
                except:
                    pass
        return

    def get_query(self, clean=1):
        """
        Creates a string for the query that it is used to create the  Cooccur
        Signal

        :param clean: shows if create the feature table for the clean or the dk
         cells

        :return a string with the query for this feature
        """
        if clean:
            self.offset = self.session.feature_count
            self.attribute_feature_id = {}
            feature_id_list = []
            for attribute in self.dirty_cells_attributes:
                self.count += 1
                self.attribute_feature_id[self.count + self.offset] = attribute
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
            self.session.feature_count += self.count
        return []
