from featurizer import Featurizer
from holoclean.global_variables import GlobalVariables
from pyspark.sql.types import *



__metaclass__ = type


class SignalCooccur(Featurizer):
    """
    This class is a subclass of the Featurizer class and
    will return the mysql query which represent the  Cooccur signal for
     the clean and dk cells
    """

    def __init__(self, session, attr_constrained):

        """

        :param attr_constrained: list of attributes that are part of a dc
        :param dataengine: a connector to database
        :param dataset: list of tables name
        """

        self.session = session
        dataengine = self.session.holo_env.dataengine
        dataset = self.session.dataset
        super(SignalCooccur, self).__init__(dataengine, dataset)
        self.id = "SignalCooccur"
        self.attr_constrained = attr_constrained
        self.table_name = self.dataset.table_specific_name('Init')

    def _get_constraint_attribute(self, table_name, attr_column_name):
        """
        Creates a string with a condition for only checking the attributes that
        are part of a DC

        :param  table_name: the name of the table that we need to check
        the attributes
        :param  attr_column_name: the name of the columns of table that
        we want to enforce the condition

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
        Creates a string for the query that it is used to create the  Cooccur
        Signal

        :param clean: shows if create the feature table for the clean or the dk
         cells

        :return a string with the query for this feature
        """
        if clean:
            name = "Observed_Possible_values_clean"
            table_name = "C_clean_flat"
        else:
            name = "Observed_Possible_values_dk"
            table_name = "C_dk_flat"

        # Create co-occur feature
        query_for_featurization = \
            "  " \
            "SELECT DISTINCT " \
            "t1.vid as vid, " \
            "t1.domain_id AS assigned_val," \
            "t3.feature_ind as feature, " \
            " 1 as count " \
            "FROM " + \
            self.dataset.table_specific_name(name) +\
            " t1, " + \
            self.dataset.\
            table_specific_name(table_name) + " t2, " + \
            self.dataset.\
            table_specific_name('Feature_id_map') + " t3 " \
            "WHERE t1.tid = t2.tid AND " \
            "t1.attr_name != t2.attribute AND " \
            " t3.attribute=t2.attribute AND " \
            " t3.value=t2.value AND " + \
            self._get_constraint_attribute('t1', 'attr_name')

        return [query_for_featurization]

    def get_dictionary_cooccur(self):
        """
        Creates a dictionary with cooccurance of each cell
        with the other attribute in the same row

        :return: cooccur_dic dictinary of the cooccurances
        """
        # Create co-occur feature

        df_possible_values = \
            self.dataengine.get_table_to_dataframe(
                "Init", self.dataset)

        all_attr = self.dataengine.get_schema(self.dataset, "Init").split(',')
        all_attr.remove(GlobalVariables.index_name)

        cooccur_dict_clean = {}
        cooccur_dict_dk = {}
        for row in df_possible_values.collect():
            dictionary = row.asDict()
            for attribute in all_attr:
                cellid_key = str(dictionary[GlobalVariables.index_name]) \
                         + "." + str(attribute)
                if cellid_key in self.session.pruning.cell_lookup:
                    cell_informations = \
                        self.session.pruning.cell_lookup[cellid_key]
                    vid = cell_informations[0]
                    k_ij = cell_informations[1]
                    clean = cell_informations[2]
                    if clean == 1:
                        cooccur_dict_clean[vid] = []
                        cooccur_dict_clean[vid].append(k_ij)
                        for all_other_attribute in all_attr:
                            if all_other_attribute != attribute:
                                if all_other_attribute != \
                                        GlobalVariables.index_name:
                                    cellid = str(
                                        dictionary[GlobalVariables.index_name])\
                                             + "." + str(all_other_attribute)
                                    if cellid not in \
                                            self.session.pruning.noisy_cells_id:
                                        feature_id = \
                                            self.session.feature_id_cooccur(
                                                all_other_attribute,
                                                dictionary[all_other_attribute])
                                        cooccur_dict_clean[
                                            vid].append(
                                            feature_id)
                    else:
                        cooccur_dict_dk[vid] = []
                        cooccur_dict_dk[vid].append(k_ij)
                        for all_other_attribute in all_attr:
                            if all_other_attribute != attribute:
                                if all_other_attribute != \
                                        GlobalVariables.index_name:
                                    cellid = str(
                                        dictionary[GlobalVariables.index_name])\
                                             + "." + str(all_other_attribute)
                                    if cellid not in self.\
                                            session.pruning.noisy_cells_id:
                                        feature_id = \
                                            self.session.feature_id_cooccur(
                                                all_other_attribute,
                                                dictionary[all_other_attribute])
                                        cooccur_dict_dk[vid].append(
                                            feature_id)


        return [cooccur_dict_clean, cooccur_dict_dk]

