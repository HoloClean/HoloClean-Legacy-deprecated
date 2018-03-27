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
        self.dc_objects = session.dc_objects

    def _create_all_relaxed_dc(self):
        """
        This method creates a list of all the possible relaxed DC's

        :return: a list of all the possible relaxed DC's
        """
        all_relax_dc = []
        self.attributes_list = []
        for dc_name in self.dc_objects:
            relax_dcs = self._create_relaxed_dc(self.dc_objects[dc_name])
            for relax_dc in relax_dcs:
                all_relax_dc.append(relax_dc)
        return all_relax_dc

    def _create_relaxed_dc(self, dc_object):
        """
        This method creates a list of all the relaxed DC's for a specific DC

        :param dictionary_dc: Dictionary mapping DC's to a list of their
         predicates
        :param dc_name: The dc that we want to relax

        :return: A list of all relaxed DC's for dc_name
        """
        relax_dcs = []
        index_name = GlobalVariables.index_name
        dc_predicates = dc_object.predicates
        for predicate in dc_predicates:
            component1 = predicate.components[0]
            component2 = predicate.components[1]
            full_form_components = predicate.cnf_form.split(predicate.operation)
            if not isinstance(component1, str):
                self.attributes_list.append(component1[1])
                relax_dc = "postab.tid = " + component1[0] + \
                           "." + index_name + " AND " + \
                           "postab.attr_name = '" + component1[1] + \
                           "' AND " + full_form_components[1] \
                           + predicate.operation + \
                           "postab.attr_val"

                if len(dc_object.tuple_names) > 1:
                    if isinstance(component1, list) and isinstance(
                            component2, list):

                        if component1[1] != component2[1]:
                            relax_dc = relax_dc + " AND  " + dc_object.tuple_names[0] + "." + \
                                       index_name + \
                                       " <> " + dc_object.tuple_names[1] + "." + \
                                       index_name
                        else:
                            relax_dc = relax_dc + " AND  " + dc_object.tuple_names[0] + "." + \
                                       index_name \
                                       + " < " + dc_object.tuple_names[1] + "." + \
                                       index_name
                    else:
                        relax_dc = relax_dc + " AND  " + dc_object.tuple_names[0] + "." + \
                                   index_name \
                                   + " < " + dc_object.tuple_names[1] + "." + \
                                   index_name

                for other_predicate in dc_predicates:
                    if predicate != other_predicate:
                        relax_dc = relax_dc + " AND  " + \
                                   other_predicate.cnf_form
                relax_dcs.append([relax_dc, dc_object.tuple_names])
            if not isinstance(component2, str):
                self.attributes_list.append(component2[1])
                relax_dc = "postab.tid = " + component2[0] +\
                           "." + index_name + " AND " + \
                           "postab.attr_name ='" + component2[1] +\
                           "' AND " + "postab.attr_val" + \
                           predicate.operation + \
                           full_form_components[0]
                if len(dc_object.tuple_names) > 1:
                    if isinstance(component1, list) and isinstance(
                            component2, list):
                        if component1[1] != component2[1]:
                            relax_dc = relax_dc + " AND  " + dc_object.tuple_names[0] + "." + \
                                       index_name + \
                                       " <> " + dc_object.tuple_names[1] + "." + \
                                       index_name
                        else:
                            relax_dc = relax_dc + " AND  " + dc_object.tuple_names[0] + "." + \
                                       index_name \
                                       + " < " + dc_object.tuple_names[1] + "." + \
                                       index_name
                    else:
                        relax_dc = relax_dc + " AND  " + dc_object.tuple_names[0] + "." + \
                                   index_name \
                                   + " < " + dc_object.tuple_names[1] + "." + \
                                   index_name

                for other_predicate in dc_predicates:
                    if predicate != other_predicate:
                        relax_dc = relax_dc + " AND  " + \
                                   other_predicate.cnf_form
                relax_dcs.append([relax_dc, dc_object.tuple_names])

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
                                      str(count + self.offset) \
                                      + " AS feature, " \
                                      "  count(*) as count " \
                                      "  FROM "
            for tuple_name in table_name:
                query_for_featurization += \
                    self.dataset.table_specific_name("Init") + \
                    " as " + tuple_name + ","
            query_for_featurization += self.dataset.table_specific_name(name) \
                                       + " as postab"

            query_for_featurization += " WHERE " +relax_dc + \
                     " GROUP BY postab.vid, postab.domain_id"
            dc_queries.append(query_for_featurization)

            if clean:
                feature_map.append([count + self.offset,
                                    self.attributes_list[index_dc],
                                    relax_dc, "DC"])

        if clean:
            df_feature_map_dc = self.spark_session.createDataFrame(
                feature_map, self.dataset.attributes['Feature_id_map'])
            self.dataengine.add_db_table('Feature_id_map',
                                         df_feature_map_dc, self.dataset, 1)
            self.session.feature_count += count

        return dc_queries
