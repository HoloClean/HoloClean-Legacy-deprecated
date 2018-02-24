from holoclean.utils.dcparser import DCParser
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
key = 'flight'
class Featurizer:
    """TODO.
        parent class for all the signals
        """

    def __init__(self, denial_constraints, dataengine, dataset):
        """TODO.
                Parameters
                --------
                denial_constraints,dataengine,dataset
                """
        self.denial_constraints = denial_constraints
        self.dataengine = dataengine
        self.dataset = dataset
        self.possible_table_name = self.dataset.table_specific_name(
            'Possible_values')
        self.table_name = self.dataset.table_specific_name('Init')
        self.dcp = DCParser(self.denial_constraints,dataengine,dataset)

    def get_constraint_attibute(self, table_name, attr_column_name):
        attr_costrained = self.dcp.get_constrainted_attributes(self.dataengine, self.dataset)

        specific_features = "("

        for const in attr_costrained:
            specific_features += table_name + "." + attr_column_name + " = '" + const + "' OR "

        specific_features = specific_features[:-4]
        specific_features += ")"

        return specific_features

    # Internal Method
    def _create_new_dc(self):
        """
        For each dc we change the predicates, and return the new type of dc
        """
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        dc_sql_parts = self.dcp.for_join_condition()
        new_dcs = []
        self.final_dc = []
        self.change_pred = []
        self.attributes_list = []
        for dc_part in dc_sql_parts:
            list_preds = self._find_predicates(dc_part)
            new_dc = self._change_predicates_for_query(list_preds, attributes)
            for dc in new_dc:
                new_dcs.append(dc)
                self.final_dc.append(dc_part)
        return new_dcs

    def _change_predicates_for_query(self, list_preds, attributes):
        """
                For each predicats we change it to the form that we need for the query to create the featurization table
                Parameters
                --------
                list_preds: a list of all the predicates of a dc
                attributes: a list of attributes of our initial table
                """

        operationsarr = ['<>', '<=', '>=', '=', '<', '>']
        new_pred_list = []

        for list_pred_index in range(0, len(list_preds)):
            components_preds = list_preds[list_pred_index].split('.')
            new_pred = ""
            rest_new_pred = ""
            first = 0
            for components_index in (0, len(components_preds) - 1):
                comp = components_preds[components_index].split("_")
                if len(comp) > 1:
                    if comp[1] in attributes:
                        for operation in operationsarr:
                            if operation in components_preds[components_index - 1]:
                                left_component = components_preds[components_index - 1].split(
                                    operation)
                                comp = components_preds[components_index].split("_")
                                self.attributes_list.append(
                                    "postab.attr_name= '" + comp[1] + "'")
                                new_pred = "postab.attr_val" + operation + \
                                    left_component[1] + "." + components_preds[components_index]
                                break
                        for index_pred in range(0, len(list_preds)):
                            if index_pred != list_pred_index:
                                #  new_pred=new_pred+" AND "+list_preds[k]
                                if first != 1:
                                    rest_new_pred = rest_new_pred + list_preds[index_pred]
                                    first = 1
                                else:
                                    rest_new_pred = rest_new_pred + \
                                        " AND " + list_preds[index_pred]
                        self.change_pred.append(rest_new_pred)
                        new_pred_list.append(new_pred)
        new_dcs = list([])
        new_dcs.append(new_pred_list[0] )
        for pred_index in range(1, len(new_pred_list)):
            new_dcs.append(new_pred_list[pred_index] )
        return new_dcs

    @staticmethod
    def _find_predicates(cond):
        """
        This method finds the predicates of dc"
        :param cond: a denial constrain
        :rtype: list_preds: list of predicates
        """

        list_preds = cond.split(' AND ')
        return list_preds


class SignalInit(Featurizer):
    """
    Signal for initial values
    """

    def __init__(self, denial_constraints, dataengine, dataset):
        """
        Create a Signal for initial values
        :param denial_constraints: The Session's current denial_constraints (i.e. Session.Denial_Constraints)
        :param dataengine: DataEngine used for the current HoloClean Session
        :param dataset: DataSet containing the current Session ID
        """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalInit"

    def get_query(self, clean=1):
        """
        This method creates a query for the featurization table for the initial values"
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
    """TODO.
    Signal for cooccurance
    """

    def __init__(self, denial_constraints, dataengine, dataset):
        """
        Create a Signal for Co-occurances
        :param denial_constraints: The Session's current denial_constraints (i.e. Session.Denial_Constraints)
        :param dataengine: DataEngine used for the current HoloClean Session
        :param dataset: DataSet containing the current Session ID
        """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalCooccur"

    def get_query(self, clean = 1):
        """
                This method creates a query for the featurization table for the cooccurances
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
                               " SELECT * FROM ( " \
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
                               self.dataset.table_specific_name(name) + " t1, " + \
                               self.dataset.table_specific_name(c) + " t2, "+ \
                               self.dataset.table_specific_name('Feature_id_map') + " as t3 " \
                                                                               " WHERE " \
                                                                               "t1.tid = t2.tid " \
                                                                               "AND " \
                                                                               "t1.attr_name != t2.attribute " \
                                                                               " AND " \
                                                                               " t3.attribute=t2.attribute " \
                                                                               " AND " \
                                                                               " t3.value=t2.value" \
                                                                               ") " \
                                                                               "AS table1;"
        self.dataengine.query(query_init_flat_join)

        # Create co-occur feature

        query_for_featurization = " (SELECT DISTINCT " \
                                  "cooccur.vid_first as vid, " \
                                  "cooccur.val_first AS assigned_val, " \
                                  " feature_ind AS feature, " \
                                  " 1 as count " \
                                  "FROM " \
                                  + self.dataset.table_specific_name(init_flat) + \
                                  " AS cooccur  " +\
                                  "WHERE " + self.get_constraint_attibute('cooccur', 'attr_first')
        return query_for_featurization


class SignalDC(Featurizer):
    """TODO.
        Signal for dc
        """

    def __init__(self, denial_constraints, dataengine, dataset, spark_session):
        """
        Create a Signal for Denial Constraints
        :param denial_constraints: The Session's current denial_constraints (i.e. Session.Denial_Constraints)
        :param dataengine: DataEngine used for the current HoloClean Session
        :param dataset: DataSet containing the current Session ID
        :param spark_session: the Spark Session contained by the HoloClean Session
        """
        self.spark_session = spark_session
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalDC"

    def get_query(self, clean=1, dcquery_prod = None):
        """
        This method creates a query for the featurization table for the dc"
        """
        join_table_name = self.dataset.table_specific_name('Init_join')
        if clean:
            name = "Possible_values_clean"
        else:
            name = "Possible_values_dk"
        self.possible_table_name = self.dataset.table_specific_name(name)

        new_dc = self._create_new_dc()
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, 'Init')
        attributes = table_attribute_string.split(',')
        query1 = "SELECT "


        for attribute in attributes:
            query1 = query1 + "table1." + attribute + " AS first_" + \
                attribute + "," + "table2." + attribute + " AS second_" + attribute + ","
        query1 = query1[:-1]
        if clean:
            query = "CREATE TABLE " \
                    + join_table_name + \
                    " AS " \
                    "SELECT * FROM (" \
                    + query1 + \
                    " FROM " + \
                    self.table_name + " AS table1," + \
                    self.table_name + " AS table2" \
                                      " WHERE" \
                                      " table1.index!=table2.index" \
                                      ") AS jointable ;"
            self.dataengine.query(query)
        dc_queries = []

        maximum = self.dataengine.query(
            "SELECT MAX(feature_ind) as max FROM " + self.dataset.table_specific_name("Feature_id_map") +
            " WHERE Type = 'cooccur'", 1
        ).collect()[0]['max']

        map_dc = []
        count = maximum
        feature_map = []
        for index_dc in range(0, len(new_dc)):
            count = count + 1
            relax_dc = new_dc[index_dc] + self.attributes_list[index_dc]
            map_dc.append([str(count), relax_dc, self.final_dc[index_dc]])
            new_condition = new_dc[index_dc]
            query_for_featurization = "(SELECT" \
                                      " postab.vid as vid, " \
                                      "postab.domain_id AS assigned_val, "+ \
                                      str(count) + " AS feature, " \
                                      "  count(table1.second_index) as count " \
                                      "  FROM " +\
                                      join_table_name + " AS table1, " +\
                                      self.possible_table_name + " as postab" \
                                      " WHERE (" + \
                                      self.change_pred[index_dc] + " AND " +\
                                      self.attributes_list[index_dc] + " AND " +\
                                      new_condition +\
                                      " AND postab.tid=table1.first_index" \
                                      ") group by postab.vid, postab.tid,postab.attr_name," \
                                      " postab.domain_id"
            dc_queries.append(query_for_featurization)


            if dcquery_prod is not None:
                dcquery_prod.appendQuery(query_for_featurization)

            if clean:
                feature_map.append([count, self.attributes_list[index_dc], self.final_dc[index_dc], "DC"])

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

class SignalSource(Featurizer):
    def __init__(self, denial_constraints, dataengine, dataset, spark_session, clean, multiple_weights=0):
        """TODO.
        Parameters
        --------
        denial_constraints,dataengine,dataset
        """
        self.spark_session = spark_session
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalSource"
        self.create_tables(clean, multiple_weights)
        self.multiple_weights = multiple_weights

    def create_tables(self, clean, multiple_weights):
        if clean:
            maximum = self.dataengine.query(
                "SELECT MAX(feature_ind) as max FROM " + self.dataset.table_specific_name("Feature_id_map"), 1
            ).collect()[0]['max']

        if clean:
            if multiple_weights == 1:
                table_attribute_string = self.dataengine.get_schema(
                    self.dataset, 'Init')
                attributes = table_attribute_string.split(',')

                create_variable_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Attribute_temp') + \
                                              "(" \
                                              "attribute varchar(64));"
                self.dataengine.query(create_variable_table_query)
                for attribute in attributes:
                    if attribute != "index" and attribute != "src" and attribute != key:
                        mysql_query = 'INSERT INTO ' + \
                                      self.dataset.table_specific_name(
                                          'Attribute_temp') + " VALUES('" + attribute + "');"
                        self.dataengine.query(mysql_query)

                create_variable_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Sources_temp') + \
                                              "(" \
                                              "source_index INT PRIMARY KEY AUTO_INCREMENT," \
                                              "name varchar(64), attribute varchar(64));"
                self.dataengine.query(create_variable_table_query)
                mysql_query = 'INSERT INTO ' + \
                              self.dataset.table_specific_name('Sources_temp') + \
                              " SELECT * FROM (SELECT distinct NULL AS source_index," \
                              "src, attribute " \
                              " from " \
                              + self.dataset.table_specific_name('Init') + " AS table1,  " \
                              + self.dataset.table_specific_name('Attribute_temp') + " AS table2  " \
                                                                                     ") AS T0;"
                self.dataengine.query(mysql_query)

                create_variable_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Sources') + \
                                              "(" \
                                              "source_index INT," \
                                              "name varchar(64), attribute varchar(64));"
                self.dataengine.query(create_variable_table_query)
                mysql_query = 'INSERT INTO ' + \
                              self.dataset.table_specific_name('Sources') + \
                              " SELECT * FROM (SELECT DISTINCT table1.source_index+" + str(maximum) + "," \
                                                                                                      "table1.name, attribute" \
                                                                                                      " from " \
                              + self.dataset.table_specific_name('Sources_temp') + " AS table1) AS T0;"
                self.dataengine.query(mysql_query)
            else:
                create_variable_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Sources_temp') + \
                                              "(" \
                                              "source_index INT PRIMARY KEY AUTO_INCREMENT," \
                                              "name varchar(64));"
                self.dataengine.query(create_variable_table_query)
                mysql_query = 'INSERT INTO ' + \
                              self.dataset.table_specific_name('Sources_temp') + \
                              " SELECT * FROM (SELECT distinct NULL AS source_index," \
                              "src " \
                              " from " \
                              + self.dataset.table_specific_name('Init') + " AS table1  " \
                                                                           ") AS T0;"
                self.dataengine.query(mysql_query)

                create_variable_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Sources') + \
                                              "(" \
                                              "source_index INT," \
                                              "name varchar(64));"
                self.dataengine.query(create_variable_table_query)
                mysql_query = 'INSERT INTO ' + \
                              self.dataset.table_specific_name('Sources') + \
                              " SELECT * FROM (SELECT DISTINCT table1.source_index+" + str(maximum) + "," \
                                                                                                      "table1.name" \
                                                                                                      " from " \
                              + self.dataset.table_specific_name('Sources_temp') + " AS table1) AS T0;"
                self.dataengine.query(mysql_query)

        if clean == 1:

            if multiple_weights == 1:
                query_featurization = "INSERT INTO " + self.dataset.table_specific_name('Feature_id_map') + \
                                      " (" \
                                      "SELECT * FROM ( SELECT " \
                                      "source_index   AS feature_ind " \
                                      ", attribute as attribute" \
                                      ", name as value" \
                                      ", 'source' as Type" \
                                      " FROM " \
                                      + self.dataset.table_specific_name('Sources') + " AS table1 ) as T0);"
                self.dataengine.query(query_featurization)
            else:
                query_featurization = "INSERT INTO " + self.dataset.table_specific_name('Feature_id_map') + \
                                      " (" \
                                      "SELECT * FROM ( SELECT " \
                                      "source_index + " + str(maximum) + " AS feature_ind " \
                                                                         ", 'null' as attribute" \
                                                                         ", name as value" \
                                                                         ", 'source' as Type" \
                                                                         " FROM " \
                                      + self.dataset.table_specific_name('Sources_temp') + " AS table1 ) as T0);"
                self.dataengine.query(query_featurization)
            return

    def get_query(self, clean=1, query_prod=None):
        if clean:
            name = "Observed_Possible_values_clean"
            possible_values = "Possible_values_clean"
        else:
            name = "Observed_Possible_values_dk"
            possible_values = "Possible_values_dk"
        self.possible_table_name = self.dataset.table_specific_name(name)
        self.possible_values = self.dataengine.get_table_to_dataframe(possible_values, self.dataset).collect()

        source_queries = []
        for possible_value in self.possible_values:
            if self.multiple_weights:
                query_for_featurization = "(SELECT " + str(possible_value.vid) + " as vid, " \
                                          " '" + str( possible_value.domain_id) + "' as assigned_val, " \
                                          " source_index as feature, " \
                                          " 1 as count FROM " + self.dataset.table_specific_name(
                                          'Init') + " i" \
                                          " INNER JOIN " + self.dataset.table_specific_name('Sources') + " s" \
                                          " ON i.src = s.name where " + key + "= (SELECT " + key + \
                                          " FROM " + self.dataset.table_specific_name('Init') + " where `index`=" + \
                                          str(possible_value.tid) + ") and " +  \
                                          " s.attribute = " + str(possible_value.attr_name) + " and " + \
                                          str(possible_value.attr_name) + "='" + \
                                          str(possible_value.attr_val) + \
                                          "' "
            else:
                query_for_featurization = "(SELECT " + str(possible_value.vid) + " as vid, " \
                                          " '" + str(possible_value.domain_id) + "' as assigned_val, " \
                                          " source_index as feature, " \
                                          " 1 as count FROM " + self.dataset.table_specific_name(
                                          'Init') + " i" \
                                          " INNER JOIN " + self.dataset.table_specific_name('Sources') + " s" \
                                          " ON i.src = s.name where " + key + "= (SELECT " + key + \
                                          " FROM " + self.dataset.table_specific_name('Init') + " where `index`=" + \
                                          str(possible_value.tid) + ") and " + str(possible_value.attr_name) \
                                          + "='" + str(possible_value.attr_val) + \
                                          "' "
            source_queries.append(query_for_featurization)

            if query_prod is not None:
                query_prod.appendQuery(query_for_featurization)
        return source_queries

