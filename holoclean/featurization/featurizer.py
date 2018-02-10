from holoclean.utils.dcparser import DCParser


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
                                    "possible_table.attr_name= '" + comp[1] + "'")
                                new_pred = "possible_table.attr_val" + operation + \
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
        new_dcs.append("(" + new_pred_list[0] + ")")
        for pred_index in range(1, len(new_pred_list)):
            new_dcs.append("(" + new_pred_list[pred_index] + ")")
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

    def pointers(self):
        create_variable_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Random_index') + \
                                      "(" \
                                      "variable_index INT PRIMARY KEY AUTO_INCREMENT," \
                                      "rv_ind LONGTEXT," \
                                      "rv_attr LONGTEXT);"
        self.dataengine.query(create_variable_table_query)
        mysql_query = 'INSERT INTO ' + \
                      self.dataset.table_specific_name('Random_index') + \
                      " SELECT * FROM (SELECT NULL AS variable_index," \
                      "table1.tid," \
                      "table2.index from " \
                      + self.dataset.table_specific_name('Init_flat') + " AS table1 , " \
                      + self.dataset.table_specific_name('Map_schema') + " AS table2, " \
                      + self.dataset.table_specific_name('C_clean') + " AS table3 " \
                                                                         "WHERE " \
                      + self.get_constraint_attibute('table1', 'attr_name') +\
                                                                         " and table1.attr_name = table2.attribute " \
                                                                         " and table3.attr =table1.attr_name and" \
                                                                         " table3.ind = table1.tid  " \
                                                                           ") AS T0;"

        self.dataengine.query(mysql_query)

        create_feature_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Feature') + \
                                     "(" \
                                     "var_index INT," \
                                     "rv_index LONGTEXT," \
                                     "rv_attr LONGTEXT," \
                                     "assigned_val INT," \
                                     "feature INT," \
                                     "TYPE LONGTEXT," \
                                     "weight_id TEXT," \
                                     "count INT" \
                                     ");"

        self.dataengine.query(create_feature_table_query)

        # Creating new weight table by joining the initial table and calculated weights
        query_featurization = "INSERT INTO " + self.dataset.table_specific_name('Feature') + \
                              " (" \
                              "SELECT * FROM ( SELECT " \
                              " variable_index AS var_index" \
                              " , table1.rv_index" \
                              " , table1.rv_attr" \
                              " , table1.assigned_val" \
                              " , table1.feature" \
                              " , table1.TYPE" \
                              " ,  table1.weight_id" \
                              " , table1.count" \
                              " FROM " \
                              + self.dataset.table_specific_name('Feature_clean') + " AS table1, " \
                              + self.dataset.table_specific_name('Random_index') + " AS table2 " \
                                                                                  " WHERE " \
                                                                                  " table1.rv_index=table2.rv_ind" \
                                                                                  " AND " \
                                                                                  "table1.rv_attr=table2.rv_attr) " \
                                                                                  "AS ftmp " \
                                                                                  ");"

        self.dataengine.query(query_featurization)

        query_featurization = "INSERT INTO " + self.dataset.table_specific_name('offset') + \
                              " (" \
                              "SELECT * FROM ( SELECT " \
                              " 'N' as offset_type" \
                              " , max(var_index) as offset" \
                              " FROM " \
                              + self.dataset.table_specific_name('Feature') + " AS table1) " \
                                                                                  "AS f);"
        self.dataengine.query(query_featurization)


    # Setters
    def add_weights(self):
        """
        This method updates the values of weights for the featurization table"
        """

        # Create internal weight table for join to calculated weights
        query_for_weights = "CREATE TABLE " \
                            + self.dataset.table_specific_name('weight_temp') \
                            + "(" \
                              "weight_id INT PRIMARY KEY AUTO_INCREMENT," \
                              "rv_attr LONGTEXT," \
                              "feature LONGTEXT" \
                              ");"

        self.dataengine.query(query_for_weights)

        # Insert initial weights to the table
        query = "INSERT INTO  " \
                + self.dataset.table_specific_name('weight_temp') + \
                " (" \
                "SELECT * FROM (" \
                "SELECT distinct NULL, rv_attr,feature FROM " + \
                self.dataset.table_specific_name('Feature_clean') + "" \
                                                                   ") AS TABLE1);"

        self.dataengine.query(query)

        create_feature_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Feature') + \
                                     "(" \
                                     "var_index INT PRIMARY KEY AUTO_INCREMENT," \
                                     "rv_index LONGTEXT," \
                                     "rv_attr LONGTEXT," \
                                     "assigned_val LONGTEXT," \
                                     "feature LONGTEXT," \
                                     "TYPE LONGTEXT," \
                                     "weight_id INT," \
                                     "count INT" \
                                     ");"

        self.dataengine.query(create_feature_table_query)

        # Creating new weight table by joining the initial table and calculated weights
        query_featurization = "INSERT INTO " + self.dataset.table_specific_name('Feature') + \
                              " (" \
                              "SELECT * FROM ( SELECT " \
                              "NULL AS var_index" \
                              " , table1.rv_index" \
                              " , table1.rv_attr" \
                              " , table1.assigned_val" \
                              " , table1.feature" \
                              " , table1.TYPE" \
                              " ,  table2.weight_id" \
                              " , table1.count" \
                              " FROM " \
                              + self.dataset.table_specific_name('Feature_clean') + " AS table1, " \
                              + self.dataset.table_specific_name('weight_temp') + " AS table2 " \
                                                                                  " WHERE" \
                                                                                  " table1.feature=table2.feature" \
                                                                                  " AND " \
                                                                                  "table1.rv_attr=table2.rv_attr) " \
                                                                                  "AS ftmp " \
                                                                                  "ORDER BY rv_index,rv_attr);"

        self.dataengine.query(query_featurization)

    def get_domain_count(self):
        """
        This method creates Domain table which is the size of the each attribute
        :return:
        """
        create_domain_table = "CREATE TABLE " + self.dataset.table_specific_name('Domain') + \
                              " (" \
                              "attr_name LONGTEXT," \
                              "cardinal LONGTEXT" \
                              ");"
        self.dataengine.query(create_domain_table)

        all_attributes_list = self.dcp.get_all_attribute(self.dataengine, self.dataset)

        for attr in all_attributes_list:
            insert_attr_count_query = "INSERT INTO  " +\
                                      self.dataset.table_specific_name('Domain') + \
                                      " SELECT '" + attr + \
                                      "' AS attr_name,(SELECT COUNT(DISTINCT " + attr + \
                                      ") AS cardinal FROM " + \
                                      self.dataset.table_specific_name('Init') + \
                                      ") AS cardinal;"
            self.dataengine.query(insert_attr_count_query)


class SignalInit(Featurizer):
    """TODO.
    Signal for initial values
    """

    def __init__(self, denial_constraints, dataengine, dataset):
        """TODO.
        Parameters
        --------
        denial_constraints,dataengine,dataset
        """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalInit"

    def get_query(self, name = "Possible_values_clean"):
        """
        This method creates a query for the featurization table for the initial values"
        """
        query_for_featurization = """ (SELECT  @p := @p + 1 AS var_index,\
            init_flat.vid as vid,
            init_flat.tid AS rv_index,\
            init_flat.attr_name AS rv_attr,\
            init_flat.domain_id AS assigned_val,\
            '1' AS feature,\
            'init' AS TYPE,\
            '      ' AS weight_id , 1 as count\
            FROM """ +\
            self.dataset.table_specific_name(name) +\
            " AS init_flat " +\
            "WHERE " + self.get_constraint_attibute('init_flat', 'attr_name') +" and init_flat.observed=1"
        return query_for_featurization


class SignalCooccur(Featurizer):
    """TODO.
    Signal for cooccurance
    """

    def __init__(self, denial_constraints, dataengine, dataset):
        """TODO.
                Parameters
                --------
                denial_constraints,dataengine,dataset
                """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalCooccur"

    def get_query(self , name = "Possible_values_clean"):
        """
                This method creates a query for the featurization table for the cooccurances
        """
        # Create cooccure table

        query_init_flat_join = "CREATE TABLE " + \
                               self.dataset.table_specific_name('Init_flat_join') + \
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
                               self.dataset.table_specific_name('C_clean_flat') + " t2, "+ \
                               self.dataset.table_specific_name('Feature_id_map') + " as t3 " \
                                                                               " WHERE " \
                                                                               "t1.tid = t2.tid " \
                                                                               "AND " \
                                                                               "t1.attr_name != t2.attribute " \
                                                                                  " AND " \
                                                                                  " t3.attribute=t2.attribute " \
                                                                                  " AND " \
                                                                                  " t3.value=t2.value" \
                                                                                    " AND" \
                                                                                    " t1.observed=1 ) " \
                                                                               "AS table1;"
        self.dataengine.query(query_init_flat_join)

        # Create co-occur feature

        query_for_featurization = " (SELECT DISTINCT @p := @p + 1 AS var_index," \
                                  "cooccur.vid_first as vid, " \
                                  "cooccur.tid_first AS rv_index," \
                                  "cooccur.attr_first AS rv_attr," \
                                  "cooccur.val_first AS assigned_val," \
                                  " feature_ind AS feature," \
                                  "'cooccur' AS TYPE," \
                                  "'        ' AS weight_id, 1 as count " \
                                  "FROM " \
                                  + self.dataset.table_specific_name('Init_flat_join') + \
                                  " AS cooccur  " +\
                                  "WHERE " + self.get_constraint_attibute('cooccur', 'attr_first')
        return query_for_featurization


class SignalDC(Featurizer):
    """TODO.
        Signal for dc
        """

    def __init__(self, denial_constraints, dataengine, dataset, spark_session):
        """TODO.
        Parameters
        --------
        denial_constraints,dataengine,dataset
        """
        self.spark_session = spark_session
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalDC"

    def get_query(self, name= "Possible_values_clean"):
        """
                This method creates a query for the featurization table for the dc"
                """

        self.possible_table_name = self.dataset.table_specific_name(name)

        new_dc = self._create_new_dc()
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, 'Init')
        attributes = table_attribute_string.split(',')
        join_table_name = self.dataset.table_specific_name('Init_join')
        query1 = "SELECT "


        for attribute in attributes:
            query1 = query1 + "table1." + attribute + " AS first_" + \
                attribute + "," + "table2." + attribute + " AS second_" + attribute + ","
        query1 = query1[:-1]

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
            "SELECT MAX(feature_ind) as max FROM " + self.dataset.table_specific_name("Feature_id_map"), 1
        ).collect()[0]['max']

        map_dc = []
        count = maximum
        for index_dc in range(0, len(new_dc)):
            count = count + 1
            relax_dc = new_dc[index_dc] + self.attributes_list[index_dc]
            map_dc.append([str(count), relax_dc, self.final_dc[index_dc]])
            new_condition = new_dc[index_dc]
            query_for_featurization = "(SELECT" \
                                      " @p := @p + 1 AS var_index," \
                                      "possible_table.vid as vid," \
                                      "possible_table.tid AS rv_index," \
                                      "possible_table.attr_name AS rv_attr,"\
                                      "possible_table.domain_id AS assigned_val,"+\
                                      str(count) + " AS feature," \
                                      "'FD' AS TYPE," \
                                      "'       ' AS weight_id ,  count(table1.second_index) as count " \
                                      "  FROM " \
                                      "(SELECT * FROM " + \
                                      join_table_name + " AS table1 " \
                                      "WHERE " + self.change_pred[index_dc] + ") AS table1," \
                                      " (SELECT * FROM " + self.possible_table_name + " AS possible_table" \
                                      " WHERE " + \
                                      self.attributes_list[index_dc] + " ) AS possible_table " +\
                                      "WHERE (" + \
                                      new_condition +\
                                      " and possible_table.tid=table1.first_index" \
                                      ") group by possible_table.vid,possible_table.tid,possible_table.attr_name," \
                                      " possible_table.domain_id"
            dc_queries.append(query_for_featurization)


            insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name(
            'Feature_id_map') + ' (feature_ind, attribute,value) Values ('+str(count)+',"' + self.attributes_list[index_dc] \
                                  +'","'+self.final_dc[index_dc]+'");'
            self.dataengine.query(insert_signal_query)




        return dc_queries
