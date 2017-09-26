class DCParser:
    
    operationsArr=['=' , '<' , '>' , '<>' , '<=' ,'>=']
    operationSign=['EQ','LT', 'GT','IQ','LTE', 'GTE']
    
    
    
    def __init__(self,denial_constraints):
        self.denial_constraints=denial_constraints
    
    def dc2SqlCondition(self):
        
        """
        Creates list of list of sql predicates by parsing the input denial constraints
        :return: list[list[string]]
        """

        
        dcSql=[]
        usedOperations=[]
        numOfContraints=len(self.denial_constraints)
        for i in range(0,numOfContraints):
            ruleParts=self.denial_constraints[i].split('&')
            firstTuple=ruleParts[0]
            secondTuple=ruleParts[1]
            numOfpredicate=len(ruleParts)-2
            dcOperations=[]
            dc2sqlpred=[]
            for c in range(2,len(ruleParts)):
                dc2sql=''
                predParts=ruleParts[c].split('(')
                op=predParts[0]
                dcOperations.append(op)
                predBody=predParts[1][:-1]
                tmp=predBody.split(',')
                predLeft=tmp[0]
                predRight=tmp[1]
                #predicate type detection
                if firstTuple in predBody and secondTuple in predBody:
                    if firstTuple in predLeft:
                        dc2sql= dc2sql+'table1.'+ predLeft.split('.')[1]+ self.operationsArr[self.operationSign.index(op)]+'table2.'+predRight.split('.')[1]
                    else:
                        dc2sql= dc2sql+'table2.'+ predLeft.split('.')[1]+ self.operationsArr[self.operationSign.index(op)]+'table1.'+predRight.split('.')[1]
                elif firstTuple in predBody:
                    if firstTuple in predLeft:
                        dc2sql= dc2sql+'table1.'+ predLeft.split('.')[1]+ self.operationsArr[self.operationSign.index(op)]+predRight
                    else:
                        dc2sql= dc2sql+ predLeft+ self.operationsArr[self.operationSign.index(op)]+'table1.'+ predRight.split('.')[1]
                else:
                    if secondTuple in predLeft:
                        dc2sql= dc2sql+'table2.'+ predLeft.split('.')[1]+ self.operationsArr[self.operationSign.index(op)]+predRight
                    else:
                        dc2sql= dc2sql+ predLeft+ self.operationsArr[self.operationSign.index(op)]+'table2.'+ predRight.split('.')[1]
                dc2sqlpred.append(dc2sql)
            usedOperations.append(dcOperations)
            dcSql.append(dc2sqlpred) 
        return dcSql,usedOperations
    
    def make_and_condition(self,conditionInd = 'all'):
        """
        return and string or list of string for conditions
        :param conditionInd: int
        :return: string or list[string]
        """
        if conditionInd == 'all':
            andlist=[]
            result,dc=self.dc2SqlCondition()
            for parts in result:
                strRes=str(parts[0])
                if len(parts)>1:
                    for i in range(1,len(parts)):
                        strRes=strRes+" AND "+str(parts[i])
                andlist.append(strRes)
            return andlist
        
        else:
            result,dc=self.dc2SqlCondition()
            parts=result[conditionInd]
            strRes=str(parts[0])
            if len(parts)>1:
                for i in range(1,len(parts)):
                    strRes=strRes+" AND "+str(parts[i])
            return strRes 
        