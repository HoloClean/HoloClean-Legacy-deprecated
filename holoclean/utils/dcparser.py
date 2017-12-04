
class DCParser:

    """TODO:
    This class parse the DC in format of
    <first tuple>&<second tuple>&<first predicate>&<second predicate>...
    and create strings that SQL comprehensive
    """

    operationsArr = ['=', '<', '>', '<>', '<=', '>=']
    operationSign = ['EQ', 'LT', 'GT', 'IQ', 'LTE', 'GTE']

    def __init__(self, denial_constraints):
        self.denial_constraints = denial_constraints

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
        usedOperations = []
        numOfContraints = len(self. denial_constraints)

        for dc_count in range(0, numOfContraints):
            # Divide the string by & cause the meaningful parts separated
            ruleParts = self.denial_constraints[dc_count].split('&')
            firstTuple = ruleParts[0]  # first tuple identifier
            secondTuple = ruleParts[1]  # second tuple identifier
            # calculating the number of predicate 2 is because of identifiers
            numOfpredicate = len(ruleParts)-2
            dcOperations = []
            dc2sqlpred = []  # list of SQL predicate
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
                dc2sqlpred.append(dc2sql)  # add the predicate to list

            usedOperations.append(dcOperations)

            dcSql.append(dc2sqlpred)

        return dcSql, usedOperations


    # Setters:

    # Getters:

    def for_join_condition(self):
        result = []
        dcs = self.get_anded_string(conditionInd='all')
        for dc in dcs:
            tmp = dc.replace('table1.','table1.first_')
            tmp = tmp.replace('table2.','table1.second_')
            result.append(tmp)
        return result


    def get_anded_string(self, conditionInd='all'):

        """
        Return and string or list of string for conditions which is and of
        predicates with SQL format
        :param conditionInd: int
        :return: string or list[string]
        """

        if conditionInd == 'all':
            andlist = []
            result, dc = self._dc_to_sql_condition()
            for parts in result:
                strRes = str(parts[0])
                if len(parts) > 1:
                    for i in range(1, len(parts)):
                        strRes = strRes+" AND "+str(parts[i])
                andlist.append(strRes)
            return andlist

        else:
            result, dc = self._dc_to_sql_condition()
            parts = result[conditionInd]
            strRes = str(parts[0])
            if len(parts) > 1:
                for i in range(1, len(parts)):
                    strRes = strRes+" AND "+str(parts[i])
            return strRes

    @staticmethod
    def get_attribute(cond, all_table_attribuites):

        """
        Return list of attribute in the give denial constraint
        :param cond: string
        :return: list[string]
        """

        attributes = set()
        for attribute in all_table_attribuites:
            if attribute in cond:
                attributes.add(attribute)

        return list(attributes)
