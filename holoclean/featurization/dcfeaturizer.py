from featurizer import Featurizer
from holoclean.global_variables import GlobalVariables

__metaclass__ = type


class SignalDC(Featurizer):
    """
    This class is a subclass of the Featurizer class and
    will return a list of mysql queries which represent the DC Signal for the
    clean and dk cells
    """

    def __init__(self, denial_constraints, session):

        """

        :param denial_constraints: list of denial_constraints
        :param session: a Holoclean session
        """

        super(SignalDC, self).__init__(session)
        self.id = "SignalDC"
        self.denial_constraints = denial_constraints
        self.spark_session = session.holo_env.spark_session
        self.parser = session.parser
        self.table_name = self.dataset.table_specific_name('Init')

    def _create_all_relaxed_dc(self):
        """
        This method creates a list of all the possible relaxed DC's

        :return: a list of all the possible relaxed DC's
        """
        all_dcs = self.parser.get_CNF_of_dcs(self.denial_constraints)
        all_relax_dc = []
        self.attributes_list = []
        dictionary_dc = self.parser.create_dc_map(self.denial_constraints)
        for dc in all_dcs:
            relax_dcs = self._create_relaxed_dc(dictionary_dc, dc)
            for relax_dc in relax_dcs:
                all_relax_dc.append(relax_dc)
        return all_relax_dc

    def _comparison_table_name(self, name):
        """
        This method chooses the appropriate name of the table for the query

        :param name: shows the name of table that we have on the comparison

        :return return the name of the table that we will use in the query
        """
        if name == "t1":
            table_name = "t2"
        else:
            table_name = "t1"
        return table_name

    def _create_relaxed_dc(self, dictionary_dc, dc_name):
        """
        This method creates a list of all the relaxed DC's for a specific DC

        :param dictionary_dc: Dictionary mapping DC's to a list of their
         predicates
        :param dc_name: The dc that we want to relax

        :return: A list of all relaxed DC's for dc_name
        """
        relax_dcs = []
        index_name = GlobalVariables.index_name
        dc_predicates = dictionary_dc[dc_name]
        for predicate_index in range(0, len(dc_predicates)):
            predicate_type = dc_predicates[predicate_index][4]
            operation = dc_predicates[predicate_index][1]
            component1 = dc_predicates[predicate_index][2]
            component2 = dc_predicates[predicate_index][3]
            # predicate_type 0 : we do not have a literal in this predicate
            # predicate_type 1 : literal on the left side of the predicate
            # predicate_type 2 : literal on the right side of the predicate
            if predicate_type == 0:
                relax_indices = range(2, 4)
            elif predicate_type == 1:
                relax_indices = range(3, 4)
            elif predicate_type == 2:
                relax_indices = range(2, 3)
            else:
                raise ValueError(
                    'predicate type can only be 0: '
                    'if the predicate does not have a literal'
                    '1: if the predicate has a literal in the left side,'
                    '2: if the predicate has a literal in right side'
                )
            for component_index in relax_indices:
                name_attribute = \
                    dc_predicates[predicate_index][component_index].split(".")
                self.attributes_list.append(name_attribute[1])
                table_name = self._comparison_table_name(name_attribute[0])
                if component_index == 2:
                    relax_dc = "postab.tid = " + name_attribute[0] +\
                               "." + index_name + " AND " + \
                               "postab.attr_name ='" + name_attribute[1] +\
                               "' AND " + "postab.attr_val" + operation + \
                               component2

                    name_attribute_temp = component2.split(".")
                else:
                    relax_dc = "postab.tid = " + name_attribute[0] + \
                               "." + index_name + " AND " + \
                               "postab.attr_name = '" + name_attribute[1] + \
                               "' AND " + component1 + operation + \
                               "postab.attr_val"

                    name_attribute_temp = component1.split(".")

                if predicate_type == 0:
                    if name_attribute_temp[1] != name_attribute[1]:
                        relax_dc = relax_dc + " AND  t1." + \
                                   index_name +\
                                   " <> t2." + \
                                   index_name
                    else:
                        relax_dc = relax_dc + " AND  t1." + \
                                   index_name \
                                   + " < t2." + \
                                   index_name
                else:
                    relax_dc = relax_dc + " AND  t1." + \
                               index_name \
                               + " < t2." + \
                               index_name

                for predicate_index_temp in range(0, len(dc_predicates)):
                    if predicate_index_temp != predicate_index:
                        relax_dc = relax_dc + " AND  " + \
                                   dc_predicates[predicate_index_temp][0]
                relax_dcs.append([relax_dc, table_name])
        return relax_dcs

    def get_query(self, clean=1, dcquery_prod=None):
        """
        Creates a list of strings for the queries that are used to create the
        DC Signal

        :param clean: shows if we create the feature table for the clean or the
        dk cells
        :param dcquery_prod: a thread that we will produce the final queries

        :return a list of strings for the queries for this feature
        """
        if clean:
            name = "Possible_values_clean"
        else:
            name = "Possible_values_dk"
        possible_table_name = self.dataset.table_specific_name(name)

        all_relax_dcs = self._create_all_relaxed_dc()
        dc_queries = []
        count = 0
        if clean:
            self.offset = self.session.feature_count

        feature_map = []
        for index_dc in range(0, len(all_relax_dcs)):
            relax_dc = all_relax_dcs[index_dc][0]
            table_name = all_relax_dcs[index_dc][1]
            count += 1
            query_for_featurization = "SELECT" \
                                      " postab.vid as vid, " \
                                      "postab.domain_id AS assigned_val, " + \
                                      str(count + self.offset) + " AS feature, " \
                                      "  count(" + table_name + \
                                      "." + GlobalVariables.index_name +\
                                      ") as count " \
                                      "  FROM " + \
                                      self.dataset. \
                                      table_specific_name('Init') + \
                                      " as t1 ," + \
                                      self.dataset. \
                                      table_specific_name('Init') + \
                                      " as t2," + \
                                      possible_table_name + " as postab" \
                                      " WHERE (" + \
                                      relax_dc + \
                                      ") GROUP BY postab.vid, postab.domain_id"
            dc_queries.append(query_for_featurization)

            if clean:
                feature_map.append([count + self.offset, self.attributes_list[index_dc],
                                    relax_dc, "DC"])

        if clean:
            df_feature_map_dc = self.spark_session.createDataFrame(
                feature_map, self.dataset.attributes['Feature_id_map'])
            self.dataengine.add_db_table('Feature_id_map',
                                         df_feature_map_dc, self.dataset, 1)
            self.session.feature_count += count

        return dc_queries
