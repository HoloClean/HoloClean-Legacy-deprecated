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
                self.dataset.table_specific_name('Feature_temp') + "" \
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
                              + self.dataset.table_specific_name('Feature_temp') + " AS table1, " \
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

    def get_query(self):
        """
        This method creates a query for the featurization table for the initial values"
        """
        query_for_featurization = """ (SELECT  @p := @p + 1 AS var_index,\
            init_flat.tid AS rv_index,\
            init_flat.attr_name AS rv_attr,\
            init_flat.attr_val AS assigned_val,\
            concat('Init=',init_flat.attr_val ) AS feature,\
            'init' AS TYPE,\
            '      ' AS weight_id , 1 as count\
            FROM """ +\
            self.dataset.table_specific_name('Init_flat') +\
            " AS init_flat " \
            "WHERE " + self.get_constraint_attibute('init_flat', 'attr_name')
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

    def get_query(self):
        """
                This method creates a query for the featurization table for the cooccurances
        """
        # Create cooccure table

        query_init_flat_join = "CREATE TABLE " + \
                               self.dataset.table_specific_name('Init_flat_join') + \
                               " SELECT * FROM ( " \
                               "SELECT DISTINCT " \
                               "t1.tid AS tid_first," \
                               "t1.attr_name AS attr_first," \
                               "t1.attr_val AS val_first," \
                               "t2.tid AS tid_second," \
                               "t2.attr_name AS attr_second," \
                               "t2.attr_val AS val_second " \
                               "FROM " + \
                               self.dataset.table_specific_name('Init_flat') + " t1," + \
                               self.dataset.table_specific_name('C_clean') + " clean," + \
                               self.dataset.table_specific_name('Init_flat') + " t2 " \
                                                                               "WHERE " \
                                                                               "t1.tid = t2.tid " \
                                                                               "AND " \
                                                                               "t2.attr_name = clean.attr " \
                                                                               "AND " \
                                                                               "t2.tid = clean.ind " \
                                                                               "AND " \
                                                                               "t1.attr_name != t2.attr_name) " \
                                                                               "AS table1;"
        self.dataengine.query(query_init_flat_join)

        # Create co-occur feature

        query_for_featurization = " (SELECT DISTINCT @p := @p + 1 AS var_index," \
                                  "cooccur.tid_first AS rv_index," \
                                  "cooccur.attr_first AS rv_attr," \
                                  "cooccur.val_first AS assigned_val," \
                                  "CONCAT (cooccur.attr_second , '=' , cooccur.val_second ) AS feature," \
                                  "'cooccur' AS TYPE," \
                                  "'        ' AS weight_id, 1 as count " \
                                  "FROM " \
                                  + self.dataset.table_specific_name('Init_flat_join') + \
                                  " AS cooccur " \
                                  "WHERE " + self.get_constraint_attibute('cooccur', 'attr_first')
        return query_for_featurization


class SignalDC(Featurizer):
    """TODO.
        Signal for dc
        """

    def __init__(self, denial_constraints, dataengine, dataset):
        """TODO.
        Parameters
        --------
        denial_constraints,dataengine,dataset
        """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalDC"

    def get_query(self):
        """
                This method creates a query for the featurization table for the dc"
                """
        new_dc = self._create_new_dc()
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Init")
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

        for index_dc in range(0, len(new_dc)):
            new_condition = new_dc[index_dc]
            query_for_featurization = "(SELECT" \
                                      " @p := @p + 1 AS var_index," \
                                      "possible_table.tid AS rv_index," \
                                      "possible_table.attr_name AS rv_attr," \
                                      "possible_table.attr_val AS assigned_val," \
                                      "CONCAT ( '" + self.final_dc[index_dc] + "') " \
                                                                                                         "AS feature," \
                                      "'FD' AS TYPE," \
                                      "'       ' AS weight_id ,  count(table1.second_index) as count " \
                                      "  FROM " \
                                      "(SELECT * FROM " + \
                                      join_table_name + " AS table1 " \
                                      "WHERE " + self.change_pred[index_dc] + ") AS table1," \
                                      " (SELECT * FROM " + self.possible_table_name + " AS possible_table" \
                                      " WHERE " + \
                                      self.attributes_list[index_dc] + " ) AS possible_table " \
                                      "WHERE (" + \
                                      new_condition + " AND" \
                                                      " possible_table.tid=table1.first_index" \
                                                      ") group by possible_table.tid,possible_table.attr_name , " \
                                                      "possible_table.attr_val"
            dc_queries.append(query_for_featurization)

        return dc_queries
