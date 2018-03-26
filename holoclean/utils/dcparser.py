from holoclean.global_variables import GlobalVariables


class DCParser:
    """TODO:
    This class parse the DC in format of
    <first tuple>&<second tuple>&<first predicate>&<second predicate>...
    and create strings that SQL comprehensive
    """

    operationsArr = ['<>', '<=', '>=', '=', '<', '>', ]
    operationSign = ['IQ', 'LTE', 'GTE', 'EQ', 'LT', 'GT']
    tables_name = ['t1', 't2']

    def __init__(self, denial_constraints):
        self.denial_constraints = denial_constraints

    # Private methods:

    def _dc_to_sql_condition(self):

        """
        Creates list of listdc_parser.operationsArr of sql predicates by
        parsing the input denial constraints
        the standard form for the is like
        't_i&t_j&EQ(t_i.a,t_j.a)&IQ(t_i.b,t_j.b)' or
        't1&t2&EQ(t1.part_counter,t2.part_counter)&IQ(t1.a,t2.a)' or
        't1&t2&EQ(t1.city,t2.city)&
        EQ(t1.temp,t2.temp)&IQ(t1.tempType,t2.tempType)'

        :return: list[list[string]]
        """
        
        dcSql = []
        usedOperations = []
        num_of_contraints = len(self.denial_constraints)

        for dc_count in range(0, num_of_contraints):
            # Divide the string by & cause the meaningful parts separated
            rule_parts = self.denial_constraints[dc_count].split('&')
            first_tuple = rule_parts[0]  # first tuple identifier
            second_tuple = rule_parts[1]  # second tuple identifier
            # calculating the number of predicate 2 is because of identifiers
            dc_operations = []
            dc2sqlpred = []

            for part_counter in range(2, len(rule_parts)):
                dc2sql = ''  # current predicate
                predParts = rule_parts[part_counter].split('(')
                op = predParts[0]  # operation appear before '('
                # set of operation
                dc_operations. \
                    append(self.operationsArr[self.operationSign.index(op)])
                predBody = predParts[1][:-1]
                tmp = predBody.split(',')
                predLeft = tmp[0]
                predRight = tmp[1]
                # predicate type detection
                if first_tuple in predBody and second_tuple in predBody:
                    if first_tuple in predLeft:
                        dc2sql = dc2sql + self.tables_name[0] + '.' + \
                                 predLeft.split('.')[1] \
                                 + self.operationsArr[
                                     self.operationSign.index(op)] \
                                 + self.tables_name[1] + '.' + \
                                 predRight.split('.')[1]
                    else:
                        dc2sql = dc2sql + self.tables_name[1] + '.' + \
                                 predLeft.split('.')[1] \
                                 + self.operationsArr[
                                     self.operationSign.index(op)] \
                                 + self.tables_name[0] + '.' + \
                                 predRight.split('.')[1]
                elif first_tuple in predBody:
                    if first_tuple in predLeft:
                        dc2sql = dc2sql + self.tables_name[0] + '.' + \
                                 predLeft.split('.')[1] \
                                 + self.operationsArr[
                                     self.operationSign.index(op)] \
                                 + predRight
                    else:
                        dc2sql = dc2sql + predLeft \
                                 + self.operationsArr[
                                     self.operationSign.index(op)] \
                                 + self.tables_name[0] + '.' + \
                                 predRight.split('.')[1]
                else:
                    if second_tuple in predLeft:
                        dc2sql = dc2sql + self.tables_name[1] + '.' + \
                                 predLeft.split('.')[1] \
                                 + self.operationsArr[
                                     self.operationSign.index(op)] \
                                 + predRight
                    else:
                        dc2sql = dc2sql + predLeft \
                                 + self.operationsArr[
                                     self.operationSign.index(op)] \
                                 + self.tables_name[1] + '.' + \
                                 predRight.split('.')[1]

                dc2sqlpred.append(dc2sql)  # add the predicate to list

            usedOperations.append(dc_operations)

            dcSql.append(dc2sqlpred)
        return dcSql, usedOperations

    def get_anded_string(self, condition_ind='all'):

        """
        Return and string or list of string for conditions which is and of
        predicates with SQL format
        :param condition_ind: int
        :return: string or list[string]
        """
        if condition_ind == 'all':
            andlist = []
            result, dc = self._dc_to_sql_condition()
            count = 0
            for parts in result:
                str_res = str(parts[0])
                if len(parts) > 1:
                    for i in range(1, len(parts)):
                        str_res = str_res + " AND " + str(parts[i])
                andlist.append(str_res)
                count += 1
            return andlist

        else:
            result, dc = self._dc_to_sql_condition()
            parts = result[condition_ind]
            str_res = str(parts[0])
            if len(parts) > 1:
                for i in range(1, len(parts)):
                    str_res = str_res + " AND " + str(parts[i])
            return str_res

    @staticmethod
    def get_attribute(cond, all_table_attribuites):

        """
        Returning a list of attributes in the given denial constraint
        :param all_table_attribuites: String
        :param cond: string
        :return: list[string]
        """

        attributes = set()
        for attribute in all_table_attribuites:
            temp = "." + attribute
            if temp in cond:
                attributes.add(attribute)

        return list(attributes)

    @staticmethod
    def get_all_attribute(dataengine, dataset):
        """
        This method return all attributes in the initial table excluding the index table
        :param dataengine:
        :param dataset:
        :return: list of all attributes
        """
        all_attributes = dataset.get_schema('Init')
        all_attributes.remove(GlobalVariables.index_name)
        return all_attributes

    def get_constraint_free_attributes(self, dataengine, dataset):
        """
        This function return all attributes that is not appeared in
        any constraints
        :param dataengine:
        :param dataset:
        :return: list of attributes
        """
        all_attributes = DCParser.get_all_attribute(dataengine, dataset)
        and_of_preds = self.get_anded_string('all')
        result = set({GlobalVariables.index_name})
        for cond in and_of_preds:
            tmp_list = self.get_attribute(cond, all_attributes)
            result = result.union(set(tmp_list))

        result = set(all_attributes).difference(result)

        return list(result)

    def get_constrainted_attributes(self, dataengine, dataset):
        """
        This function return all attributes that is appeared
        at least in one constraint

        :param dataengine:
        :param dataset:

        :return: list of attributes
        """
        result = set(DCParser.get_all_attribute(dataengine, dataset))
        free_attributes = \
            self.get_constraint_free_attributes(dataengine, dataset)

        result = result.difference(set(free_attributes))

        return list(result)

    @staticmethod
    def get_operators(denial_constraint):
        operators = denial_constraint.split('&')
        operators = operators[2:]
        for i in range(0, len(operators)):
            operators[i] = operators[i].partition('(')[0]
        return operators

    @staticmethod
    def get_columns(denial_constraint):
        operators = denial_constraint.split('&')
        operators = operators[2:]
        columns = []
        for i in range(0, len(operators)):
            dc = operators[i].split('.')
            operators[i] = operators[i].partition('(')[0]
            columns.append([])
            for j in range(1, len(dc)):
                columns[i].append(dc[j].partition(',')[0].partition(')')[0])
        return columns
