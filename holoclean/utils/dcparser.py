
class DCParser:

    """TODO:
    This class parse the DC in format of
    <first tuple>&<second tuple>&<first predicate>&<second predicate>...
    and create strings that SQL comprehensive
    """

    operationsArr = ['=', '<', '>', '<>', '<=', '>=']
    operationSign = ['EQ', 'LT', 'GT', 'IQ', 'LTE', 'GTE']
    nonsymmetricOperations = ['LT', 'GT', 'LTE', 'GTE']

    def __init__(self, denial_constraints, dataengine, dataset):
        self.denial_constraints = denial_constraints
        self.contains_nonsymmetric_operator(dataengine, dataset)
    # Private methods:

    def _dc_to_sql_condition(self):

        """
        Creates list of list of sql predicates by parsing the
        input denial constraints
        the standard form for the is like
        't_i&t_j&EQ(t_i.a,t_j.a)&IQ(t_i.b,t_j.b)' or
        't1&t2&EQ(t1.part_counter,t2.part_counter)&IQ(t1.a,t2.a)' or
        't1&t2&EQ(t1.city,t2.city)&EQ(t1.temp,t2.temp)&IQ(t1.tempType,t2.tempType)'

        :return: list[list[string]]
        """

        dcSql = []
        finalnull = []

        usedOperations = []
        numOfContraints = len(self. denial_constraints)

        for dc_count in range(0, numOfContraints):
            # Divide the string by & cause the meaningful parts separated
            ruleParts = self.denial_constraints[dc_count].split('&')
            firstTuple = ruleParts[0]  # first tuple identifier
            secondTuple = ruleParts[1]  # second tuple identifier
            # calculating the number of predicate 2 is because of identifiers
            dcOperations = []
            dc2sqlpred = []
            nullsql = []  # list of SQL predicate
            for part_counter in range(2, len(ruleParts)):
                dc2sql = ''  # current predicate
                predParts = ruleParts[part_counter].split('(')
                op = predParts[0]   # operation appear before '('
                # set of operation
                dcOperations.\
                    append(self.operationsArr[self.operationSign.index(op)])
                predBody = predParts[1][:-1]
                tmp = predBody.split(',')
                predLeft = tmp[0]
                predRight = tmp[1]
                # predicate type detection
                if firstTuple in predBody and secondTuple in predBody:
                    if firstTuple in predLeft:
                        dc2sql = dc2sql + 'table1.' + predLeft.split('.')[1] \
                            + self.operationsArr[self.operationSign.index(op)]\
                            + 'table2.' + predRight.split('.')[1]
                    else:
                        dc2sql = dc2sql + 'table2.' + predLeft.split('.')[1]\
                            + self.operationsArr[self.operationSign.index(op)]\
                            + 'table1.' + predRight.split('.')[1]
                elif firstTuple in predBody:
                    if firstTuple in predLeft:
                        dc2sql = dc2sql+'table1.' + predLeft.split('.')[1]\
                            + self.operationsArr[self.operationSign.index(op)]\
                            + predRight
                    else:
                        dc2sql = dc2sql + predLeft\
                            + self.operationsArr[self.operationSign.index(op)]\
                            + 'table1.' + predRight.split('.')[1]
                else:
                    if secondTuple in predLeft:
                        dc2sql = dc2sql + 'table2.' + predLeft.split('.')[1]\
                            + self.operationsArr[self.operationSign.index(op)]\
                            + predRight
                    else:
                        dc2sql = dc2sql + predLeft\
                            + self.operationsArr[self.operationSign.index(op)]\
                            + 'table2.' + predRight.split('.')[1]
                nulsql = 'table1.' + predLeft.split('.')[1] + " IS NULL"
                nullsql.append(nulsql)
                dc2sqlpred.append(dc2sql)  # add the predicate to list

            usedOperations.append(dcOperations)

            dcSql.append(dc2sqlpred)
            finalnull.append(nullsql)

        return dcSql, usedOperations, finalnull

    # Setters:

    # Getters:

    def for_join_condition(self):
        result = []
        dcs, nothing = self.get_anded_string(conditionInd='all')
        for dc in dcs:
            tmp = dc.replace('table1.', 'table1.')
            tmp = tmp.replace('table2.', 'table2.')
            result.append(tmp)
        return result

    def get_anded_string(self, conditionInd='all'):

        """
        Return and string or list of string for conditions which is and of
        predicates with SQL format
        :param conditionInd: int
        :return: string or list[string]
        """
        nulllist = []
        if conditionInd == 'all':
            andlist = []
            nulllist = []
            result, dc, nullsql = self._dc_to_sql_condition()
            count = 0
            for parts in result:
                strRes = str(parts[0])
                if len(parts) > 1:
                    for i in range(1, len(parts)):
                        strRes = strRes+" AND "+str(parts[i])
                andlist.append(strRes)
                count += 1
            for sql1 in nullsql:
                for null_part in sql1:
                    strRes1 = ""
                    strRes1 += str(null_part)
                nulllist.append(strRes1)
            return andlist, nulllist

        else:
            result, dc = self._dc_to_sql_condition()
            parts = result[conditionInd]
            strRes = str(parts[0])
            if len(parts) > 1:
                for i in range(1, len(parts)):
                    strRes = strRes+" AND "+str(parts[i])
            return strRes, nulllist

    @staticmethod
    def get_attribute(cond, all_table_attribuites):

        """
        Return list of attribute in the give denial constraint
        :param cond: string
        :return: list[string]
        """

        attributes = set()
        for attribute in all_table_attribuites:
            temp = "." + attribute
            if temp in cond:
                attributes.add(attribute)

        return list(attributes)

    def get_all_attribute(self, dataengine, dataset):
        """
        This method return all attributes in the initial table
        :param dataengine:
        :param dataset:
        :return: list of all attributes
        """
        all_list = dataengine.get_schema(dataset, "Init")
        all_attributes = all_list.split(',')
        all_attributes.remove('index')
        return all_attributes

    def get_constraint_free_attributes(self, dataengine, dataset):
        """
        This function return all attributes that is not appeared in
        any constraints
        :param dataengine:
        :param dataset:
        :return: list of attributes
        """
        all_attributes = self.get_all_attribute(dataengine, dataset)
        and_of_preds, nothing = self.get_anded_string('all')
        result = set({'index'})
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
        result = set(self.get_all_attribute(dataengine, dataset))
        free_attributes = \
            self.get_constraint_free_attributes(dataengine, dataset)

        result = result.difference(set(free_attributes))

        return list(result)

    # Given ONE denial constraint will return array of corresponding operators
    # Example:
    #   't1&t2&EQ(t1.State,t2.State)&EQ(t1.MeasureCode,t2.MeasureCode)&IQ(t1.Stateavg,t2.Stateavg)'
    # will output ['EQ', 'EQ', 'IQ']
    @staticmethod
    def get_operators(denial_constraint):
        operators = denial_constraint.split('&')
        operators = operators[2:]
        for i in range(0, len(operators)):
            dc = operators[i].split('.')
            operators[i] = operators[i].partition('(')[0]
        return operators

    # Given ONE denial constraint will return array of column names
    # Example:
    #   't1&t2&EQ(t1.State,t2.State)&EQ(t1.MeasureCode,t2.MeasureCode)&IQ(t1.Stateavg,t2.Stateavg)'
    # will output:
    #   [['State', 'State'], ['MeasureCode', 'MeasureCode'], ['Stateavg', 'Stateavg']]
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

    # Checks through all denial constraints to see if it contains a non-symmetric operator
    # If a denial constraint does contain a non-symmetric operator then it will altar
    # the corresponding columns to be of type INTEGER
    def contains_nonsymmetric_operator(self, dataengine, dataset):
        for dc in self.denial_constraints:
            operators = DCParser.get_operators(dc)
            columns = DCParser.get_columns(dc)
            for index in range(0, len(operators)):
                operator = operators[index]
                if self.nonsymmetricOperations.count(operator) != 0:
                    for column in columns[index]:
                        dataengine.altar_column(dataset, column)
