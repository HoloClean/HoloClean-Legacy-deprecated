from numbskull.numbskulltypes import *


class Wrapper:
    def __init__(self, dataengine, dataset):
        """TODO.
                Parameters
                --------
                dataset, dataengine
                """
        self.dataset = dataset
        self.dataengine = dataengine
        self._make_dictionary()

    # Internal method
    def _make_dictionary(self):
        """
                This method creates a dictionary for each attribute

        """

        mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('Possible_values') + \
                      " AS " \
                      "(SELECT DISTINCT "\
                      "rv_index as tid," \
                      "rv_attr as attr_name," \
                      "assigned_val  as attr_val " \
                      " FROM " + \
                      self.dataset.table_specific_name('Feature')+");"

        self.dataengine.query(mysql_query)

        domain_dataframe = self.dataengine.get_table_to_dataframe(
            "Possible_values", self.dataset)
        temp = domain_dataframe.select("tid", "attr_name", "attr_val").collect()

        self.dictionary = {}
        dic_list = []
        for row in temp:
            dic_list.append(row.asDict())
        attr_set = set()
        for element in dic_list:
            attr_set.add((element["tid"], element["attr_name"]))
        attr_set = list(attr_set)
        result = {}
        for a in attr_set:
            domain_dict = {}
            result.update({a: domain_dict})
        element_id = [0] * len(attr_set)
        for element in dic_list:
            result[(element["tid"], element["attr_name"])].update(
                {(element["attr_val"]): element_id[attr_set.index((element["tid"], element["attr_name"]))]})
            element_id[attr_set.index((element["tid"], element["attr_name"]))] += 1
        self.dictionary = result

    # Setters
    def set_weight(self):
        """
                This method creates a query for weight table for the factor

        """

        mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('Weights') + \
                      " AS " \
                      "(SELECT DISTINCT (0 + table1.weight_id) AS weight_id ," \
                      "0 AS Is_fixed," \
                      "RAND()*(1+1)-1 AS init_val" \
                      " FROM " + \
                      self.dataset.table_specific_name('Feature') + " AS table1" + \
                                                                    " GROUP BY table1.weight_id);"

        self.dataengine.query(mysql_query)
        return

    def set_variable(self):
        """
                This method creates a query for variable table for numbskull

                """

        # Set global counter query and Creating Variable table query

        create_vartmp_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Variable_tmp') + \
                                    "(" \
                                    "variable_index INT," \
                                    "rv_ind LONGTEXT," \
                                    "rv_attr LONGTEXT," \
                                    "is_Evidence LONGTEXT," \
                                    "initial_value LONGTEXT," \
                                    "Datatype LONGTEXT," \
                                    "Cardinality INT," \
                                    "vtf_offset TEXT" \
                                    ");"

        self.dataengine.query(create_vartmp_table_query)

        mysql_query = "INSERT INTO " + \
                      self.dataset.table_specific_name('Variable_tmp') + \
                      "(SELECT (@c := @c + 1) AS variable_index," \
                      " table2.rv_index AS rv_ind," \
                      "table2.rv_attr AS rv_attr," \
                      "'0' AS is_Evidence," \
                      "'0' AS initial_value," \
                      "'1' AS Datatype," \
                      "count1 AS Cardinality," \
                      "'       ' AS vtf_offset" \
                      " FROM " \
                      + "( SELECT  rv_index , rv_attr FROM " + self.dataset.table_specific_name('Feature') +\
                        " group by rv_index,rv_attr)" + " AS table2," \
                        "(SELECT count(*) AS count1," \
                        "attr_name,tid " \
                        "FROM " \
                      + self.dataset.table_specific_name('Possible_values') + \
                      " GROUP BY attr_name,tid) AS counting," \
                      "(SELECT @c:=0) m " \
                      "WHERE " \
                      "counting.attr_name=table2.rv_attr " \
                      "AND " \
                      "counting.tid=table2.rv_index);"
        self.dataengine.query(mysql_query)

        mysql_query = 'CREATE TABLE ' + \
                      self.dataset.table_specific_name('Feature_gb') + " AS" \
                      "(SELECT MIN(var_index) AS smallest, rv_index, rv_attr " \
                      " FROM " + self.dataset.table_specific_name('Feature') +\
                      " GROUP BY rv_index, rv_attr);"
        self.dataengine.query(mysql_query)

        # Create tmp table and update attribute vtf_offset

        create_variable_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Variable') + \
                                      "(" \
                                      "variable_index INT PRIMARY KEY AUTO_INCREMENT," \
                                      "rv_ind LONGTEXT," \
                                      "rv_attr LONGTEXT," \
                                      "is_Evidence LONGTEXT," \
                                      "initial_value LONGTEXT," \
                                      "Datatype LONGTEXT," \
                                      "Cardinality LONGTEXT," \
                                      "vtf_offset LONGTEXT" \
                                      ");"
        self.dataengine.query(create_variable_table_query)

        mysql_query = 'INSERT INTO ' + \
                      self.dataset.table_specific_name('Variable') + \
                      " SELECT * FROM (SELECT NULL AS variable_index," \
                      "table2.rv_ind as rv_ind," \
                      "table2.rv_attr," \
                      "table2.is_Evidence," \
                      "table2.initial_value," \
                      "table2.Datatype," \
                      "table2.Cardinality," \
                      "table1.smallest AS vtf_offset FROM  " \
                      + self.dataset.table_specific_name('Feature_gb') + " AS table1 , " \
                      + self.dataset.table_specific_name('Variable_tmp') + " AS table2 " \
                                                                           "WHERE " \
                                                                           "table1.rv_index = table2.rv_ind " \
                                                                           "AND " \
                                                                           "table1.rv_attr = table2.rv_attr" \
                                                                           ") AS vtmp ORDER BY vtf_offset;"

        self.dataengine.query(mysql_query)

    def set_factor_to_var(self):
        """
                This method creates a query for factor_to_variable table for numbskull

                """
        create_ftv_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Factor_to_var') + \
                                 "(" \
                                 "factor_to_var_index INT PRIMARY KEY AUTO_INCREMENT," \
                                 "vid LONGTEXT," \
                                 "rv_ind LONGTEXT," \
                                 "attr_val LONGTEXT," \
                                 "attr_name LONGTEXT" \
                                 ");"
        self.dataengine.query(create_ftv_table_query)

        mysql_query = 'INSERT INTO ' + \
                      self.dataset.table_specific_name('Factor_to_var') + \
                      " SELECT * FROM ((SELECT NULL AS factor_to_var_index," \
                      "variable_index AS vid," \
                      "rv_ind," \
                      "attr_val," \
                      "table1.attr_name" \
                      " FROM  " \
                      + self.dataset.table_specific_name('Possible_values') + " AS table1," \
                      + self.dataset.table_specific_name('Variable') + " AS table2," \
                                                                       " (SELECT @n:=0) m " \
                                                                       "WHERE " \
                                                                       "table1.tid=rv_ind " \
                                                                       "AND" \
                                                                       " table1.attr_name=table2.rv_attr)) " \
                                                                       "AS ftvtmp;"

        self.dataengine.query(mysql_query)

    def set_factor(self):
        """
                This method creates a query for factor table for numbskull"

                """
        create_factor_table_query = "CREATE TABLE " + self.dataset.table_specific_name('Factor') + \
                                    "(" \
                                    "factor_index INT PRIMARY KEY AUTO_INCREMENT," \
                                    "var_index LONGTEXT," \
                                    "FactorFunction LONGTEXT," \
                                    "weightID LONGTEXT," \
                                    "Feature_Value LONGTEXT," \
                                    "arity LONGTEXT," \
                                    "ftv_offest LONGTEXT" \
                                    ");"
        self.dataengine.query(create_factor_table_query)

        mysql_query = 'INSERT INTO ' + \
                      self.dataset.table_specific_name('Factor') + \
                      " SELECT * FROM ((SELECT distinct NULL AS factor_index," \
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
                                                                            ")ORDER BY var_index) AS ftmp ;"

        self.dataengine.query(mysql_query)

    # Getters
    def get_list_weight(self):
        """
                This method creates list of weights for numbskull

                """
        weight_dataframe = self.dataengine.get_table_to_dataframe(
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
        variable_dataframe = self.dataengine.get_table_to_dataframe(
            "Variable", self.dataset)
        temp = variable_dataframe.select(
            "rv_ind",
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
                    int(self.dictionary[(int(tempdictionary["rv_ind"]), tempdictionary["rv_attr"])][
                            tempdictionary["initial_value"]])),
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
        factor_to_var_dataframe = self.dataengine.get_table_to_dataframe(
            "Factor_to_var", self.dataset)
        temp = factor_to_var_dataframe.select(
            "vid", "rv_ind", "attr_val", "attr_name").collect()
        factor_to_var_list = []
        for row in temp:
            tempdictionary = row.asDict()
            factor_to_var_list.append([np.int64(tempdictionary["vid"]), np.int64(
                self.dictionary[(tempdictionary["rv_ind"], tempdictionary["attr_name"])][
                    tempdictionary["attr_val"]])])
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
        factor_dataframe = self.dataengine.get_table_to_dataframe(
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

    @staticmethod
    def get_edge(factor_list):
        """
                This method returns the number of edges for numbskull

                """
        edges = len(factor_list)
        return edges

    @staticmethod
    def get_mask(variable_list):
        """
                This method returns the domain mask for numbskull

                """
        mask = np.zeros(len(variable_list), np.bool)
        return mask
