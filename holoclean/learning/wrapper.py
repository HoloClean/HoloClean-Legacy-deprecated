from numbskull.numbskulltypes import *


class Wrapper:
    def __init__(self, dataengine, dataset):
        """TODO.
                Parameters
                --------
                parameter: denial_constraints, dataengine
                """
        self.dataset = dataset
        self.dataengine = dataengine
        self._make_dictionary()

    # Internal method
    def _make_dictionary(self):
        """
                This method creates a dictionary for each attribute

                """

        domain_dataframe = self.dataengine._table_to_dataframe(
            "Domain", self.dataset)
        temp = domain_dataframe.select("attr_name", "attr_val").collect()
        self.dictionary = {}
        dic_list = []
        for row in temp:
            dic_list.append(row.asDict())
        attr_set = set()
        for element in dic_list:
            attr_set.add(element["attr_name"])
        attr_set = list(attr_set)
        result = {}
        for a in attr_set:
            domain_dict = {}
            result.update({a: domain_dict})
        element_id = [0] * len(attr_set)
        for element in dic_list:
            result[element["attr_name"]].update(
                {element["attr_val"]: element_id[attr_set.index(element["attr_name"])]})
            element_id[attr_set.index(element["attr_name"])] += 1
        self.dictionary = result

    # Setters
    def set_weight(self):
        """
                This method creates a query for weight table for the factor

                """
        # All the weights that are  fixed

        mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('Weights') + \
                      " AS " \
                      "(SELECT (0 + table1.weight_id) AS weight_id ," \
                      "1 AS Is_fixed," \
                      "1  AS init_val" \
                      " FROM " + \
                      self.dataset.table_specific_name('Feature') + " AS table1" \
                                                                    " WHERE TYPE='init'" \
                                                                    " GROUP BY table1.weight_id);"
        self.dataengine.query(mysql_query)
        # All the weights that are not fixed
        mysql_query = "INSERT INTO  " + self.dataset.table_specific_name('Weights') +\
                       " (SELECT (" \
                       "0 + table2.weight_id) AS weight_id ," \
                       "0 AS Is_fixed, 0 AS init_val" \
                       " FROM " + \
                       self.dataset.table_specific_name('Feature') + \
                       " AS table2 " \
                       "WHERE TYPE!='init' " \
                       "GROUP BY table2.weight_id); "
        self.dataengine.query(mysql_query)

    def set_variable(self):
        """
                This method creates a query for variable table for numbskull

                """

        mysql_query = 'CREATE TABLE ' + \
            self.dataset.table_specific_name('Variable') + ' AS'
        mysql_query += "(SELECT NULL AS variable_index," \
                       " table1.tid AS rv_ind," \
                       "table1.attr_name AS rv_attr," \
                       "'0' AS is_Evidence," \
                       "0 AS initial_value," \
                       "'1' AS Datatype" \
                       ",count1 AS Cardinality" \
                       ", '       ' AS vtf_offset" \
                       " FROM " + self.dataset.table_specific_name('Possible_values') + " AS table1," \
                       + self.dataset.table_specific_name('C_dk') + " AS table2," \
                                                                    "(SELECT count(*) AS count1," \
                                                                    "attr_name " \
                                                                    "FROM " \
                       + self.dataset.table_specific_name('Domain') + \
                       " GROUP BY(attr_name)) AS counting" \
                       " WHERE" \
                       " table1.tid=table2.ind " \
                       "AND" \
                       " table1.attr_name=table2.attr" \
                       " AND " \
                       "counting.attr_name=table1.attr_name)"
        print mysql_query

        table_attribute_string = self.dataengine._get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')

        for attribute in attributes:
            if attribute != "index":
                # Query for each attribute
                mysql_query += "UNION " \
                               "(SELECT NULL AS variable_index," \
                               "table1.index AS rv_ind," \
                               "table2.attr AS rv_attr," \
                               "'1' AS is_Evidence ," + \
                               attribute + " AS initial_value," \
                                           "'1' AS Datatype," \
                                           "count1 AS Cardinality," \
                                           " '       ' AS vtf_offset" \
                                           "  FROM " + self.dataset.table_specific_name('Init') + " AS table1, " \
                               + self.dataset.table_specific_name('C_clean') + " AS table2," \
                                                                               " (SELECT count(*) AS count1," \
                                                                               "attr_name " \
                                                                               "FROM " \
                               + self.dataset.table_specific_name('Domain') \
                               + " GROUP BY(attr_name)) AS counting" \
                                 " WHERE" \
                                 " table2.ind=table1.index " \
                                 "AND " \
                                 "table2.attr='" + attribute + "'" \
                                                               " AND" \
                                                               " counting.attr_name='" + attribute + "')"
        # Update attribute vtf_offset
        mysql_query += "ORDER BY rv_ind,rv_attr;" \
                       "ALTER TABLE " + self.dataset.table_specific_name('Variable') + " MODIFY variable_index" \
                                                                                       " INT AUTO_INCREMENT PRIMARY KEY;" \
                                                                                       "UPDATE " \
                       + self.dataset.table_specific_name('Variable') + \
                       " SET vtf_offset = " \
                       "(SELECT min(var_index) AS offset" \
                       " FROM " + self.dataset.table_specific_name('Feature') + " AS table1" \
                                                                                " WHERE  " + \
                       self.dataset.table_specific_name('Variable') + ".rv_ind=table1.rv_index" \
                                                                      " AND " \
                       + self.dataset.table_specific_name('Variable') \
                       + ".rv_attr= table1.rv_attr " \
                         "GROUP BY rv_index,rv_attr) ;"
        self.dataengine.query(mysql_query)

    def set_factor_to_var(self):
        """
                This method creates a query for factor_to_variable table for numbskull

                """
        mysql_query = 'CREATE TABLE ' + \
            self.dataset.table_specific_name('Factor_to_var') + ' AS'
        mysql_query += "(SELECT (@n := @n + 1 ) AS factor_to_var_index," \
                       " variable_index AS vid," \
                       "attr_val," \
                       "table1.attr_name" \
                       " FROM  " \
                       + self.dataset.table_specific_name('Possible_values') + " AS table1," \
                       + self.dataset.table_specific_name('Variable') + " AS table2," \
                                                                        " (SELECT @n:=0) m " \
                                                                        "WHERE " \
                                                                        "table1.tid=rv_ind " \
                                                                        "AND" \
                                                                        " table1.attr_name=table2.rv_attr);"
        self.dataengine.query(mysql_query)

    def set_factor(self):
        """
                This method creates a query for factor table for numbskull"

                """
        mysql_query = 'CREATE TABLE ' + \
            self.dataset.table_specific_name('Factor') + ' AS'
        mysql_query += "(SELECT distinct (@n := @n + 1 ) AS factor_index," \
                       "var_index," \
                       " '4' AS FactorFunction," \
                       " table1.weight_id AS weightID," \
                       " '1' AS Feature_Value," \
                       " '1' AS arity," \
                       " table3.factor_to_var_index AS ftv_offest" \
                       " FROM " + self.dataset.table_specific_name('Feature') + " AS table1," \
                       + self.dataset.table_specific_name('Variable') + " AS table2," \
                       + self.dataset.table_specific_name('Factor_to_var') + " AS table3 ," \
                                                                             " (SELECT @n:=0) m " \
                                                                             "WHERE " \
                                                                             "table1.rv_index=table2.rv_ind " \
                                                                             "AND " \
                                                                             "table1.rv_attr= table2.rv_attr " \
                                                                             "AND " \
                                                                             "table3.vid=table2.variable_index " \
                                                                             "AND " \
                                                                             "table3.attr_val=table1.assigned_val " \
                                                                             "ORDER BY var_index);"
        self.dataengine.query(mysql_query)

    # Getters
    def get_list_weight(self):
        """
                This method creates list of weights for numbskull

                """
        weight_dataframe = self.dataengine._table_to_dataframe(
            "Weights", self.dataset)
        temp = weight_dataframe.select("Is_fixed", "init_val").collect()
        weight_list = []
        for row in temp:
            tempdictionary = row.asDict()
            weight_list.append(
                [(tempdictionary["Is_fixed"]), tempdictionary["init_val"]])
        weight = np.zeros(len(weight_list), Weight)
        count = 0
        for w in weight:
            w["isFixed"] = weight_list[count][0]
            w["initialValue"] = weight_list[count][1]
            count += 1

        return weight

    def get_list_variable(self):
        """
                This method creates list of variables for numbskull

                """
        variable_dataframe = self.dataengine._table_to_dataframe(
            "Variable", self.dataset)
        temp = variable_dataframe.select(
            "rv_attr",
            "is_Evidence",
            "initial_value",
            "Datatype",
            "Cardinality",
            "vtf_offset").collect()
        variable_list = []
        for row in temp:
            tempdictionary = row.asDict()
            if int(tempdictionary["is_Evidence"]) == 0:
                variable_list.append([np.int8(int(tempdictionary["is_Evidence"])),
                                      np.int64((int(0))),
                                      np.int16(int(tempdictionary["Datatype"])),
                                      np.int64(int(tempdictionary["Cardinality"])),
                                      np.int64(int(tempdictionary["vtf_offset"]))])
            else:
                variable_list.append([np.int8(int(tempdictionary["is_Evidence"])), np.int64(
                    (int(self.dictionary[tempdictionary["rv_attr"]][tempdictionary["initial_value"]]))),
                    np.int16(int(tempdictionary["Datatype"])),
                    np.int64(int(tempdictionary["Cardinality"])),
                    np.int64(int(tempdictionary["vtf_offset"]))])
            variable = np.zeros(len(variable_list), Variable)
        count = 0
        for var in variable:
            var["isEvidence"] = variable_list[count][0]
            var["initialValue"] = variable_list[count][1]
            var["dataType"] = variable_list[count][2]
            var["cardinality"] = variable_list[count][3]
            var["vtf_offset"] = (variable_list[count][4] - 1)
            count += 1
        return variable

    def get_list_factor_to_var(self):
        """
                This method creates list of fmap for numbskull

                """
        factor_to_var_dataframe = self.dataengine._table_to_dataframe(
            "Factor_to_var", self.dataset)
        temp = factor_to_var_dataframe.select(
            "vid", "attr_val", "attr_name").collect()
        factor_to_var_list = []
        for row in temp:
            tempdictionary = row.asDict()
            factor_to_var_list.append([np.int64(tempdictionary["vid"]), np.int64(
                self.dictionary[tempdictionary["attr_name"]][tempdictionary["attr_val"]])])
        fmap = np.zeros(len(factor_to_var_list), FactorToVar)
        count = 0
        for f in fmap:
            f["vid"] = factor_to_var_list[count][0] - 1
            f["dense_equal_to"] = factor_to_var_list[count][1]
            count += 1
        return fmap

    def get_list_factor(self):
        """
                This method creates list of factors for numbskull

                """
        factor_dataframe = self.dataengine._table_to_dataframe(
            "Factor", self.dataset)
        temp = factor_dataframe.select(
            "FactorFunction",
            "weightID",
            "Feature_Value",
            "arity",
            "ftv_offest").collect()
        factor_list = [0] * len(temp)

        counter = 0
        for row in temp:
            tempdictionary = row.asDict()
            factor_list[counter] = [
                np.int16(
                    tempdictionary["FactorFunction"]), np.int64(
                    tempdictionary["weightID"]), np.float64(
                    tempdictionary["Feature_Value"]), np.int64(
                    tempdictionary["arity"]), np.int64(
                        (int(
                            tempdictionary["ftv_offest"]) - 1))]
            counter += 1

        factor = np.zeros(len(factor_list), Factor)

        count = 0
        for f in factor:
            f["factorFunction"] = factor_list[count][0]
            f["weightId"] = factor_list[count][1]
            f["featureValue"] = factor_list[count][2]
            f["arity"] = factor_list[count][3]
            f["ftv_offset"] = factor_list[count][4]
            count += 1

        return factor

    def get_edge(self, factor_list):
        """
                This method returns the number of edges for numbskull

                """
        edges = len(factor_list)
        return edges

    def get_mask(self, variable_list):
        """
                This method returns the domain mask for numbskull

                """
        mask = np.zeros(len(variable_list), np.bool)
        return mask
