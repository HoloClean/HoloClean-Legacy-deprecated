from holoclean.utils.dcparser import DCParser
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

key = 'flight'


class Featurizer:
    """
        parent class for all the signals
        """

    def __init__(self, denial_constraints, dataengine, dataset):
        """
        Parameters
        --------
        denial_constraints,dataengine,dataset
        """
        self.denial_constraints = denial_constraints
        self.dataengine = dataengine
        self.dataset = dataset
        self.table_name = self.dataset.table_specific_name('Init')
        self.dcp = DCParser(self.denial_constraints)


    def get_constraint_attibute(self, table_name, attr_column_name):
        attr_costrained = self.dcp.get_constrainted_attributes(
            self.dataengine, self.dataset)

        specific_features = "("

        for const in attr_costrained:
            specific_features += table_name + "." + attr_column_name + " = '" \
                                 + const + "' OR "

        specific_features = specific_features[:-4]
        specific_features += ")"
        return specific_features

        # Internal Method
    def _create_new_dc(self):
        """
        Return a list of all possible relaxed DC's for the current session
        """
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        all_dcs  = self.dcp.get_anded_string(
            conditionInd='all')
        all_relax_dc = []
        self.final_dc = []
        self.change_pred = []
        self.attributes_list = []
        dictionary_dc = self.create_dc_map(all_dcs)
        for dc in all_dcs:
            relax_dc = self._create_relaxes(dictionary_dc, dc)
            for dc in relax_dc:
                all_relax_dc.append(dc)
        return all_relax_dc

    def _create_relaxes(self, dictionary_dc, dc_name):
        """
        Will return a list of all relaxed DC's for the given DC

        :param dictionary_dc: Dictionary mapping DC's to a list of their predicates
        :param dc_name: The string of the DC we want to relax
        :return: A list of all relaxed DC's for dc_name
        """
        relax_dcs = []
        dc_predicates = dictionary_dc[dc_name]
        for predicate_index in range(0, len(dc_predicates)):
            predicate_type = dc_predicates[predicate_index][4]
            operation = dc_predicates[predicate_index][1]
            component1 = dc_predicates[predicate_index][2]
            component2 = dc_predicates[predicate_index][3]
            if predicate_type == 0:
                relax_indices = range(2, 4)
            elif predicate_type == 1:
                relax_indices = range(3, 4)
            elif predicate_type == 2:
                relax_indices = range(2, 3)
            else:
                raise ValueError('predicate type can only be 0, 1 or 2')
            for i in relax_indices:
                relax_dc = ""
                attributes = dc_predicates[predicate_index][i].split(".")
                self.attributes_list.append(attributes[1])
                table_name = self.creating_table_name(attributes[0])
                if i == 2:
                    relax_dc = "postab.attr_name ='"+attributes[1] + "' AND " + "postab.attr_val" + operation + \
                               component2 + " AND postab.tid = "+attributes[0] + ".index"
                else:
                    relax_dc = "postab.attr_name = '"+attributes[1] + "' AND " + component1 + operation + \
                    "postab.attr_val" +" AND postab.tid = "+attributes[0] + ".index"

                for predicate_index_temp in range(0, len(dc_predicates)):
                    if predicate_index_temp != predicate_index:
                        relax_dc = relax_dc + " AND  " + dc_predicates[predicate_index_temp][0]
                relax_dcs.append([relax_dc,table_name])
        return relax_dcs

    def creating_table_name(self, name):
        if name == "table1":
            table_name = "table2"
        else:
            table_name = "table1"
        return table_name

    def create_dc_map(self, dcs):
        """
        Returns a dictionary that takes a dc as a string for a key and takes
        a list of its predicates for the value

        Example Output:
        {'table1.ZipCode=table2.ZipCode)AND(table1.City,table2.City':
            [
                ['table1.ZipCode= table2.ZipCode', '=','table1.ZipCode', 'table2.ZipCode',0],
                ['table1.City<>table2.City', '<>','table1.City', 'table2.City' , 0]
            ]
        }

        :param dcs: a list of DC's with their string representation
        :return: A dictionary of mapping every dc to their list of predicates
        """
        dictionary_dc = {}
        for dc in dcs:
            list_preds = self._find_predicates(dc)
            dictionary_dc[dc] = list_preds
        return dictionary_dc

    @staticmethod
    def _find_predicates(dc):
        """
        This method finds the predicates of dc"
        input example: 'table1.ZipCode=table2.ZipCode)and(table1.City,table2.City'

        output example [['table1.ZipCode= table2.ZipCode', '=','table1.ZipCode', 'table2.ZipCode',0],
        ['table1.City<>table2.City', '<>','table1.City', 'table2.City' , 0]]

        :param dc: a string representation of the denial constraint

        :rtype: predicate_list: list of predicates and it's componenents and type:
                        [full predicate string, component 1, component 2,
                        (0=no literal or 1=component 1 is literal or 2=component 2 is literal) ]
        """

        predicate_list = []
        operations_list = ['=', '<>','<', '>',  '<=', '>=']
        predicates = dc.split(' AND ')
        for predicate in predicates:
            predicate_components = []
            type = 0
            predicate_components.append(predicate)
            for operation in operations_list:
                if operation in predicate:
                    componets = predicate.split(operation)
                    predicate_components.append(operation)
                    break

            component_index = 1
            for component in componets:
                if component.find("table1.") == -1 and component.find("table2.") == -1:
                    type = component_index
                predicate_components.append(component)
                component_index = component_index + 1

            predicate_components.append(type)
            predicate_list.append(predicate_components)

        return predicate_list

class SignalInit(Featurizer):
    """
    Signal for initial values
    """

    def __init__(self, denial_constraints, dataengine, dataset):
        """
        Create a Signal for initial values
        :param denial_constraints: The Session's current denial_constraints
        (i.e. Session.Denial_Constraints)
        :param dataengine: DataEngine used for the current HoloClean Session
        :param dataset: DataSet containing the current Session ID
        """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalInit"

    def get_query(self, clean=1):
        """
        This method creates a query for the featurization table
        for the initial values"
        """

        if clean:
            name = "Observed_Possible_values_clean"
        else:
            name = "Observed_Possible_values_dk"

        query_for_featurization = """ (SELECT  \
            init_flat.vid as vid, init_flat.domain_id AS assigned_val, \
            '1' AS feature, \
            1 as count\
            FROM """ +\
            self.dataset.table_specific_name(name) +\
            " AS init_flat " +\
            "WHERE " + self.get_constraint_attibute('init_flat', 'attr_name')
        return query_for_featurization


class SignalCooccur(Featurizer):
    """
    Signal for cooccurance
    """

    def __init__(self, denial_constraints, dataengine, dataset):
        """
        Create a Signal for Co-occurances
        :param denial_constraints: The Session's current denial_constraints
        (i.e. Session.Denial_Constraints)
        :param dataengine: DataEngine used for the current HoloClean Session
        :param dataset: DataSet containing the current Session ID
        """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalCooccur"

    def get_query(self, clean=1):
        """
                This method creates a query for the featurization table
                for the cooccurances
        """
        # Create cooccure table

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


class SignalDC(Featurizer):
    """TODO.
        Signal for dc
        """

    def __init__(self, denial_constraints, dataengine, dataset, spark_session):
        """
        Create a Signal for Denial Constraints
        :param denial_constraints: The Session's current
            denial_constraints (i.e. Session.Denial_Constraints)
        :param dataengine: DataEngine used for the current HoloClean Session
        :param dataset: DataSet containing the current Session ID
        :param spark_session: the Spark Session contained
            by the HoloClean Session
        """
        self.spark_session = spark_session
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalDC"

    def get_query(self, clean=1, dcquery_prod=None):
        """
        This method creates a query for the featurization table for the dc"
        """
        if clean:
            name = "Possible_values_clean"
        else:
            name = "Possible_values_dk"
        self.possible_table_name = self.dataset.table_specific_name(name)

        all_relax_dcs = self._create_new_dc()
        dc_queries = []

        if clean:
            count = self.dataengine.query(
                "SELECT COALESCE(MAX(feature_ind), 0) as max FROM " +
                self.dataset.table_specific_name("Feature_id_map") +
                " WHERE Type != 'DC'", 1
            ).collect()[0]['max']
            count += 1
        else:
            count = self.dataengine.query(
                "SELECT COALESCE(MIN(feature_ind), 0) as max FROM " +
                self.dataset.table_specific_name("Feature_id_map") +
                " WHERE Type = 'DC'", 1
            ).collect()[0]['max']

        map_dc = []
        feature_map = []
        for index_dc in range(0, len(all_relax_dcs)):
            relax_dc = all_relax_dcs[index_dc][0]
            table_name = all_relax_dcs[index_dc][1]
            query_for_featurization = "(SELECT" \
                                      " postab.vid as vid, " \
                                      "postab.domain_id AS assigned_val, " + \
                                      str(count) + " AS feature, " \
                                      "  count(" + table_name + ".index) as count " \
                                      "  FROM " + \
                                      self.dataset.\
                                      table_specific_name('Init') +\
                                      " as table1 ," + \
                                      self.dataset.\
                                      table_specific_name('Init') +\
                                      " as table2," + \
                                      self.possible_table_name + " as postab" \
                                      " WHERE (" + \
                                      " table1.index < table2.index AND " + \
                                      relax_dc +\
                                      ") GROUP BY postab.vid, postab.tid," \
                                      "postab.attr_name, postab.domain_id"
            dc_queries.append(query_for_featurization)

            if dcquery_prod is not None:
                dcquery_prod.appendQuery(query_for_featurization)

            if clean:
                feature_map.append([count, self.attributes_list[index_dc],
                                    relax_dc, "DC"])
            count += 1

        if clean:
            df_feature_map_dc = self.spark_session.createDataFrame(
                feature_map, StructType([
                    StructField("feature_ind", IntegerType(), True),
                    StructField("attribute", StringType(), False),
                    StructField("value", StringType(), False),
                    StructField("Type", StringType(), False),
                ]))
            self.dataengine.add_db_table('Feature_id_map',
                                         df_feature_map_dc, self.dataset, 1)

        return dc_queries

