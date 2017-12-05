from holoclean.utils.dcparser import DCParser


class Featurizer:
    """TODO.
        parent class for all the signals
        """

    def __init__(self, denial_constraints, dataengine, dataset):
        """TODO.
                Parameters
                --------
                parameter: denial_constraints,dataengine,dataset
                """
        self.denial_constraints = denial_constraints
        self.dataengine = dataengine
        self.dataset = dataset
        self.possible_table_name = self.dataset.table_specific_name(
            'Possible_values')
        self.table_name = self.dataset.table_specific_name('Init')

    # Internal Method
    def _create_new_dc(self):
        """
        For each dc we change the predicates, and return the new type of dc
        """
        table_attribute_string = self.dataengine._get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        dcp = DCParser(self.denial_constraints)
        dc_sql_parts = dcp.for_join_condition()
        new_dcs = []
        self.final_dc = []
        self.change_pred = []
        self.attributes_list = []
        for c in dc_sql_parts:
            list_preds = self._find_predicates(c)
            temp = self._change_predicates_for_query(list_preds, attributes)
            for dc in temp:
                new_dcs.append(dc)
                self.final_dc.append(c)
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

        for i in range(0, len(list_preds)):
            components_preds = list_preds[i].split('.')
            new_pred = ""
            new_pred1 = ""
            first = 0
            for p in (0, len(components_preds) - 1):
                comp = components_preds[p].split("_")
                if len(comp) > 1:
                    if comp[1] in attributes:
                        for operation in operationsarr:
                            if operation in components_preds[p - 1]:
                                left_component = components_preds[p - 1].split(
                                    operation)
                                comp = components_preds[p].split("_")
                                self.attributes_list.append(
                                    "possible_table.attr_name= '" + comp[1] + "'")
                                new_pred = "possible_table.attr_val" + operation + \
                                    left_component[1] + "." + components_preds[p]
                                break
                        for k in range(0, len(list_preds)):
                            if k != i:
                                #  new_pred=new_pred+" AND "+list_preds[k]
                                if first != 1:
                                    new_pred1 = new_pred1 + list_preds[k]
                                    first = 1
                                else:
                                    new_pred1 = new_pred1 + \
                                        " AND " + list_preds[k]
                        self.change_pred.append(new_pred1)
                        new_pred_list.append(new_pred)
        new_dc = ""
        new_dcs = []
        new_dc = new_dc + "(" + new_pred_list[0] + ")"
        new_dcs.append("(" + new_pred_list[0] + ")")
        for i in range(1, len(new_pred_list)):
            new_dcs.append("(" + new_pred_list[i] + ")")
        return new_dcs

    def _find_predicates(self, cond):
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

        dataframe = self.dataengine._table_to_dataframe(
            "Feature", self.dataset)
        groups = []
        for c in dataframe.collect():
            temp = [c['rv_attr'], c['feature']]
            if temp not in groups:
                groups.append(temp)
        query = "UPDATE " + \
            self.dataset.table_specific_name('Feature') + " SET weight_id= CASE"
        for weight_id in range(0, len(groups)):
            query += " WHEN  rv_attr='" + groups[weight_id][0] + "'AND feature='" + groups[weight_id][
                1] + "' THEN " + str(weight_id)
        query += " END;"
        self.dataengine.query(query)


class SignalInit(Featurizer):
    """TODO.
    Signal for initial values
    """

    def __init__(self, denial_constraints, dataengine, dataset):
        """TODO.
        Parameters
        --------
        parameter: denial_constraints,dataengine,dataset
        """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalInit"

    def get_query(self):
        """
        This method creates a query for the featurization table for the initial values"
        """
        query_for_featurization = ""
        query_for_featurization += """ (SELECT  @p := @p + 1 AS var_index,\
            possible_table.tid AS rv_index,\
            possible_table.attr_name AS rv_attr,\
            possible_table.attr_val AS assigned_val,\
            concat('Init=',possible_table.attr_val ) AS feature,\
            'init' AS TYPE,\
            '      ' AS weight_id\
            FROM """ +\
            self.possible_table_name +\
            """ AS possible_table\
            WHERE possible_table.observed='1') UNION"""
        query_for_featurization = query_for_featurization[:-5]
        return query_for_featurization


class SignalCooccur(Featurizer):
    """TODO.
    Signal for cooccurance
    """

    def __init__(self, denial_constraints, dataengine, dataset):
        """TODO.
                Parameters
                --------
                parameter: denial_constraints,dataengine,dataset
                """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalCooccur"

    def get_query(self):
        """
                This method creates a query for the featurization table for the cooccurances
                """
        self.table_name1 = self.dataset.table_specific_name('Init_new')
        query_for_featurization = """ (SELECT  @p := @p + 1 AS var_index,\
            possible_table.tid AS rv_index,\
            possible_table.attr_name AS rv_attr,\
            possible_table.attr_val AS assigned_val,\
            concat (table1.attr_name,'=',table1.attr_val ) AS feature,\
            'cooccur' AS TYPE,'        ' AS weight_id \
            FROM""" + self.table_name1 +  """ AS table1,\
            """ + self.possible_table_name + """ AS possible_table\
            WHERE (table1.attr_name != possible_table.attr_name\
            AND\
            table1.tid = possible_table.tid )\
            )"""  # End of FROM
        return query_for_featurization


class SignalDC(Featurizer):
    """TODO.
        Signal for dc
        """

    def __init__(self, denial_constraints, dataengine, dataset):
        """TODO.
        Parameters
        --------
        parameter: denial_constraints,dataengine,dataset
        """
        Featurizer.__init__(self, denial_constraints, dataengine, dataset)
        self.id = "SignalDC"

    def get_query(self):
        """
                This method creates a query for the featurization table for the dc"
                """
        new_dc = self._create_new_dc()
        table_attribute_string = self.dataengine._get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        join_table_name = self.dataset.table_specific_name('join_init')
        query1 = "SELECT "
        for i in attributes:
            query1 = query1 + "table1." + i + " AS first_" + \
                i + "," + "table2." + i + " AS second_" + i + ","
        query1 = query1[:-1]
        query = "CREATE TABLE " + join_table_name + " AS SELECT * FROM (" + query1 + " FROM " + self.table_name + \
                " AS table1," + self.table_name + """ AS table2 WHERE table1.index!=table2.index) AS jointable ;"""
        self.dataengine.query(query)
        dc_queries = []
        for index_dc in range(0, len(new_dc)):
            new_condition = new_dc[index_dc]
            # if index_dc == 0:
            query_for_featurization = """(SELECT  @p := @p + 1 AS var_index, \
                possible_table.tid AS rv_index,\
                possible_table.attr_name AS rv_attr,\
                possible_table.attr_val AS assigned_val,\
                concat ( table1.second_index,'""" + \
                self.final_dc[index_dc] + \
                """') AS feature,'FD' AS TYPE ,'       ' AS weight_id  FROM  \
                (SELECT * FROM """ + join_table_name + \
                """ AS table1 WHERE """ + \
                self.change_pred[index_dc] + \
                """) AS table1, (SELECT * FROM """ + self.possible_table_name \
                + """ AS possible_table WHERE """ + \
                self.attributes_list[index_dc] + \
                """ ) AS possible_table WHERE (""" + new_condition + \
                """ AND possible_table.tid=table1.first_index ) )"""
            dc_queries.append(query_for_featurization)

        return dc_queries
