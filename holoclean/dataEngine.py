import pyspark as ps
import numpy as np
import itertools
import operator
import copy
from pyspark import SparkConf, SparkContext


class DomainPruning:
    
    selection_threshold=0.5 #preset value for pruning domain value
    
    def __init__(self,dataset,noisy_cells):
        self.dataset=dataset
        self.noisy_cells=noisy_cells
    
    def candidate_values(self):
        """
        This function return active domain of given noisy cells
        :type noisy_data: list[(int,string)]
        :type dataset: pd.DataFrame
        :type threshold: float
        :rtype: list[set[string]]
        """
        repair_candidates=[]
    
        for c in self.noisy_cells: #Calculate candidate value for each noisy cell
            # Initialize repair candidates
            repair_candidate = set()
            c_row = c[0]
            c_attr = c[1]
            domain_c_attr = self.attribute_active_domain(c_attr)
            for attr in list(self.dataset.columns):
                if attr != c_attr: #goes over all other attribute except noisy cell attribute
                    v_attr = self.indexValue(c_row, attr)
                    for c_v in domain_c_attr: # for each value in the domain we calculate the p value
                        # Calculate the Pr(v_c | v_attr)
                        
                        num_v_attr=len(self.dataset.filter(self.dataset[attr]==v_attr).collect())    
                        num_both=len(self.dataset.filter(self.dataset[c_attr]==c_v).filter(self.dataset[attr]==v_attr).collect())
                            
                        pr = float(num_both) / num_v_attr
                        if pr >= self.selection_threshold:
                            repair_candidate.add(c_v)
            repair_candidates.append(list(repair_candidate))
    
        return repair_candidates
    
    #This function get an attribute and return its active domain
    
    def attribute_active_domain(self,attribute):
        """
        Returns the full domain of an attribute
        :type attr: string
        :rtype: list[string]
        """
        domain = set()
        tmp=self.dataset.select(attribute).collect()
        for v in range(0,len(tmp)):
            domain.add(tmp[v].asDict()[attribute])
        return list(domain) 
    #By using this function we can change the domain pruning threshold
    
    def set_threshold(self,new_threshold):
        self.selection_threshold=new_threshold
    
    #This function create not allowed index by getting noisy cells
   
    def unallowable_index_list(self):
        return np.unique([self.noisy_cells[k][0] for k in range(0,len(self.noisy_cells))]) #Noisy attributes 
   
    
    #    This function calculate and return a list of lists that specifies valid value for each attribute in order they appeared in dataframe\
    
    def indexValue(self,ind,attr):
        """         
        Returns the value of cell with index=ind and column=attr
        :type ind: int
         :type attr: string
         :rtype: list[list[None]] 
        """
        row=self.dataset.filter(self.dataset['index']==ind).collect()
        row2dict=row[0].asDict()
        
        return row2dict[attr]
        
    def allowable_doamin_value(self):
        
        #This part calculate the over all domain
        new_domain={}
        attributes=self.dataset.columns
        for a in attributes:
            if a != 'index':        
                new_domain.update({a:np.unique([self.indexValue(k, a) for k in range(1,self.dataset.count()+1)]).tolist()})        
        #This part calculate value for noisy cells
        old_domain=copy.deepcopy(new_domain)
        allowed_value_for_noisy=self.candidate_values()
        #Attribute that are noisy
        noisy_attribute=np.unique([self.noisy_cells[k][1] for k in range(0,len(self.noisy_cells))])
        pruned_domain={a:[] for a in noisy_attribute}
        noisy_attribute_inorder=[self.noisy_cells[k][1] for k in range(0,len(self.noisy_cells))]
        #Calculate
        index_over_noisy_domain=0
        for k in noisy_attribute_inorder :
            pruned_domain.update({k:np.unique(pruned_domain[k]+allowed_value_for_noisy[index_over_noisy_domain]).tolist()})
            index_over_noisy_domain+=1
        for a in pruned_domain:
            new_domain.update({a:pruned_domain[a]})
    
        return new_domain 
    ###############################  

    def allowable_rows(self,joint_table,spark_session):
        
        """
        This function prune cells that are not allowed to be in the training data based on pruned domain and 
        noisy cells
        :type joint_table: DataFrame
        :rtype: DataFrame
        """
        
        unallowed_index=self.unallowable_index_list()
        allowed_domain_value=self.allowable_doamin_value()
        

        data=[]
        for row in joint_table.collect():
            index,column=self.cell2index(row)
            
            if index not in unallowed_index and self.indexValue(index, column) in allowed_domain_value[column]:         
                data.append(row)
        result_dataframe=spark_session.createDataFrame(data,joint_table.columns)        
        return result_dataframe
    
    def cell2index(self,row):
        tmp=row['cell']
        index=tmp['_1']
        column=tmp['_2']
        return index,column
        
                   
#This class implement all type of the pruning algorithm

class DenialConstraint:

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
    
    def make_and_condition(self,conditionInd):
        """
        return the list of indexed constraints
        :param conditionInd: int
        :return: string
        """
        result,dc=self.dc2SqlCondition()
        parts=result[conditionInd]
        strRes=str(parts[0])
        if len(parts)>1:
            for i in range(1,len(parts)):
                strRes=strRes+" AND "+str(parts[i])
        return strRes
    
    def noViolation_tuple(self,dataset,condition,spak_session):
        
        dataset.createOrReplaceTempView("df")   
        q="SELECT table1.index as indexT1,table2.index as indexT2 FROM df table1,df table2 WHERE NOT("+ self. make_and_condition(condition)+")"        
        satisfied_tuples_index=spak_session.sql(q)         
        return satisfied_tuples_index
    
    def violation_tuple(self,dataset,condition,spak_session):
        
        dataset.createOrReplaceTempView("df")   
        q="SELECT table1.index as indexT1,table2.index as indexT2 FROM df table1,df table2 WHERE ("+ self. make_and_condition(condition)+")"        
        not_satisfied_tuples_index=spak_session.sql(q)         
        return not_satisfied_tuples_index
    
    def all_dc_violation(self,dataset,spak_session):
        
        """
        Return list of violation tuples dataframe
        :param dataset: Dataframe
        :param spak_session: SparkSession        
        :return: list[Dataframe]
        """
        
        return [self.violation_tuple(dataset, i, spak_session) for i in range(0,len(self.denial_constraints))]
    
    def all_dc_nonViolation(self,dataset,spak_session):
        
        """
        Return list of non violation tuples dataframe
        :param dataset: Dataframe
        :param spak_session: SparkSession        
        :return: list[Dataframe]
        """
        
        return [self.noViolation_tuple(dataset, i, spak_session) for i in range(0,len(self.denial_constraints))]


class DenialConstraintFeaturize:
    
    #########################################################
    ################# Initialize ############################ 
    #########################################################
    
    #Set of operations that can appear in the denial constraints 

    
    #For each QuantativeStatisticsFeaturize , dataset and noisy cells are needed
    def __init__(self,dataset,denial_constraints):
        self.dataset=dataset
        self.denial_constraints=denial_constraints
   
 
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
   
    #########################################################
    ########## Make feature for multiple tuples ############# 
    #########################################################
    
    #@@@@@@@@ Main Method @@@@@@@@@@#
    
    def featurize(self,noviolations,spark_session):
        temp_dataset=self.dataset
#         num_of_rows=temp_dataset.count()*(len(temp_dataset.columns)-1)-len(self.noisy_cells)
        num_of_rows=temp_dataset.count()*(len(temp_dataset.columns)-1)
        data=[]
    
        #######Creating default data
        indexCol=self.index2list()
        for row in indexCol:
            for p in temp_dataset.columns:
#                 if self.not_noisy(row, p) and p!='index':
                if p!='index':
                    tm=[-1]*(temp_dataset.count()*len(self.denial_constraints))
                    data.append([(row, p)]+tm)
        ##########################################

        for dc_count in range(0,len(self.denial_constraints)):
            tmp=noviolations[dc_count]           
            #GO over truth value
            for i in tmp.collect():
                row_tuple=i.asDict()['indexT1']
                col_tuple=i.asDict()['indexT2']
                for tu_count in range(0,num_of_rows):
                    cell_info =data[tu_count][0]
                    curr_tuple_index=cell_info[0]
                    curr_tuple_attribute=cell_info[1]
                    
                    #Change the data arrays
                    col_changed=(dc_count) * self.dataset.count() + indexCol.index(col_tuple) + 1
                    if self.inclusion(curr_tuple_attribute,dc_count):                       
                        if int(curr_tuple_index) == int(row_tuple):
                            data[tu_count][col_changed] = 1
                    else:
                        data[tu_count][col_changed] = 0

        col_names=['cell']+[str(i) for i in range(1,temp_dataset.count()*len(self.denial_constraints)+1)]

        new_df = spark_session.createDataFrame(data,col_names)
        
        return(new_df)  
   
    
    #$$$$$$$$$$$$$$$$ Not Main $$$$$$$$$$$$$$
    
    
    
    
    """Supplementary methods"""
    
    def not_noisy(self, index , attribute,noisy_cells):
        # This function get some index and attribute and by considering the noisy cell arrays return the true if it is not noisy
        if (index,attribute) in noisy_cells:
            return False
        return True
    
    
    
    
    #This function check that this objects attribute is included in denial constraint we need it in making denial constraint put zero in place
    
    
    def inclusion (self, attribute, denial_index):
        #This function by getting denial_index return TRUE if attribute
        # included in the denial constraints that index in denial_constraint
        if attribute in self.denial_constraints[denial_index]:
            return True
        else:
            return False
    
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@   
    
    #########################################################
    ########### Make feature vector for one tuple ########### 
    #########################################################
    
    # This function make feature vector for a cell with index i  j in the dataset
    
    
    def make_featurvector(self, index, attribute ,parsedDCs,spak_session):
        numOfTuple = self.dataset.count()
        result =[0]*int(len(self.denial_constraints)*numOfTuple + 1)
        result[0] = (index , attribute)
        cc = 1
        indices=self.index2list()
        for dccount in range(0,len(parsedDCs)):
            for p in indices:
                if self.inclusion(attribute,dccount):
                    result[cc]=self.truthEval(parsedDCs[dccount], index, p, spak_session)
                else:
                    result[cc]=0
                cc+=1
        return result
    
    
    
    
    def truthEval(self,parsedCode,indexTuple1,indexTuple2,spak_session):
        #'table1.city=table2.city', 'table1.temp=table2.temp', 'table1.tempType<>table2.tempType'
        """for example we can consider t1.a1!=t2.a1 and t1.a2=t2.a2
        its code is something like a string "a1,3,a2,0"
        """
        dfLeft=self.dataset.filter(self.dataset['index']==indexTuple1)
        dfRight=self.dataset.filter(self.dataset['index']==indexTuple2)
        dfLeft.createOrReplaceTempView("dfl")
        dfRight.createOrReplaceTempView("dfr")
        
        
        trueCombination=[]
        for i in range(0,len(parsedCode)):       
            q="SELECT table1.index as indexT1,table2.index as indexT2 FROM dfl table1,dfr table2 WHERE NOT("+ parsedCode[i]+")"
            trueCombination.append(spak_session.sql(q))
        satisfied_tuples_index=trueCombination[0]
        for i in range(0,len(parsedCode)):
            satisfied_tuples_index=satisfied_tuples_index.union(trueCombination[i])        
        if satisfied_tuples_index.count()>0 :
            return 1
        return -1
        

    
    def index2list(self):        
        """
        Returns list of indices
        :rtype: list[string]
        """
        li_tmp=self.dataset.select('index').collect()
        
        return [i.asDict()['index'] for i in li_tmp ]
    
    def indexValue(self,ind,attr):
        """
        Returns the value of cell with index=ind and column=attr
        :type ind: int
        :type attr: string
        :rtype: list[list[None]]
        """
        row=self.dataset.filter(self.dataset['index']==ind).collect()
        row2dict=row[0].asDict()
        
        return row2dict[attr]    
     

class ExternalDataFeaturize:
    pass


class QuantativeStatisticsFeaturize:
    
    #For each QuantativeStatisticsFeaturize , dataset and noisy cells are needed
    def __init__(self,dataset):
        self.dataset=dataset


    # This value calculate the mean value of the cell attribute in the table   
    
    
    def average_value (self,Table,cell):
        pass
    
    
    def featurize(self,spark_session):
        
        """
        Creates dataframe of features using various statistics of the data
        :param noisy_data: List((int, string))
        :return: dataframe
        """
        
        feature_table_list = []
        lst_attributes = self.dataset.columns
        indexList=self.index2list()
        
        for i in indexList:
            for attr in lst_attributes:
                
#                 if (i, attr) not in self.noisy_cells and attr!='index':
                if  attr!='index':
                    feature_table_list.append(self.put_value_in_vector(i,attr,self.indexValue(i, attr)))
        
        new_df = spark_session.createDataFrame(feature_table_list)
        return new_df
    
    """
    Number of times the value v appeared in the dataset under attribute attr
    """
    def frequency(self , attr, v):
        
        """
        Return number repetition of attribute with given value 
        :param attr: string
        :param v
        :return: int
        """
        count= len(self.dataset.filter(self.dataset[attr]==v).collect())
        return count
    
    
    """
    Within one row, count the number of times both the attr1 has the value v 
    and the attr2 has value w
    """
    def frequency2(self, attr1, attr2, row_index):

        v = self.indexValue(row_index, attr1)
        w = self.indexValue(row_index, attr2)
    
        count= len(self.dataset.filter(self.dataset[attr1]==v).filter(self.dataset[attr2]==w).collect())
        return count
    
    
    """
    Within one row, count the number of times both the attr1 has the value v 
    and the attr2 has value w and the attr3 has value u
    """
    def frequency3(self, attr1, attr2, attr3, row_index):
        v = self.indexValue(row_index, attr1)
        w = self.indexValue(row_index, attr2)
        u = self.indexValue(row_index, attr3)
    
        count= len(self.dataset.filter(self.dataset[attr1]==v).filter(self.dataset[attr2]==w).filter(self.dataset[attr3]==u).collect())
        return count
    
    
    def put_value_in_vector(self,index,attr,v):
        
        """
        This function get all statistical value and put the in the feature vector
        :param index: int
        :param attr:  string
        :param v: Nat
        :return: list
        """
        
        lst_attributes=self.dataset.columns
        new_row = {}
        new_row['cell'] = (index, attr)
        for a in lst_attributes:
            if a!='index':
                title = 'freq_' + str(a)
                if a == attr:
                    new_row[title] = self.frequency(attr, v)
                else:
                    new_row[title] = 0
    
        for a1, a2 in itertools.combinations(lst_attributes, 2):
            if a1!='index' and a2!='index' :
                title = 'freq_2_' + a1 + "_" + a2
                if attr == a1 or attr == a2:
                    new_row[title] = self.frequency2( a1, a2, index)
                else:
                    new_row[title] = 0
    
        for a1, a2, a3 in itertools.combinations(lst_attributes, 3):
            if a1!='index' and a2!='index' and a3!='index' :
                title = 'freq_3_' + a1 + "_" + a2 + "_" + a3
                if attr == a1 or attr == a2 or attr == a3:
                    new_row[title] = self.frequency3(a1, a2, a3, index)
                else:
                    new_row[title] = 0
        return new_row

    def indexValue(self,ind,attr):
        """
        Returns the value of cell with index=ind and column=attr
        :type ind: int
        :type attr: string
        :rtype: list[list[None]]
        """
        row=self.dataset.filter(self.dataset['index']==ind).collect()
        row2dict=row[0].asDict()
        
        return row2dict[attr]
    
    def index2list(self):
        li_tmp=self.dataset.select('index').collect()
        
        return [i.asDict()['index'] for i in li_tmp ]


class HolocleanData:
    
    def __init__(self,dataset):
        self.dataset=dataset
        
#         self.noisy_cells=noisy_cells
#         self.denial_constraints=denial_constraints
    def index_X(self,feature_vector,spark_session):
        feature_vector.createOrReplaceTempView("df")   
        q="SELECT cell as  FROM df "        
        index=spark_session.sql(q)               
        x=feature_vector.drop('cell')
        
        return index,x
        
            
    def split_noisy_nonNoisy(self,featurevectors_dataset,noisy_cells,spark_session):
        """
        Returns two dataframe one for clean cells and one for noisy cells
        :type Dataframes
        :rtype: Dataframe , Dataframe
        """
        output_schema=featurevectors_dataset.columns
        noisyVectors=[]
        cleanVectors=[]
        for row in featurevectors_dataset.collect():
            index,attr=self.cell2index(row)
            if (index,attr) in noisy_cells :
                noisyVectors.append(row)
            else:
                cleanVectors.append(row)
        
        noisyDataframe=spark_session.createDataFrame(noisyVectors)
        cleanDataframe=spark_session.createDataFrame(cleanVectors)
        return noisyDataframe , cleanDataframe
        
    
    def signal_features_aggregator(self,*signals):
        
        """
        Returns join of the input tables
        :type *Dataframes
        :rtype: Dataframe
        """
        numOfSignals=len(signals)
        result=signals[0]
        if numOfSignals >1:
            for i in range(1,numOfSignals):                
                result=result.join(signals[i],'cell')
        return result
                
    def make_trainingdata_label(self,feature_vectors,spark_session):
               
        """
        Returns label dataframe which is the Y for training data
        :type Dataframes
        :rtype: Dataframe
        """ 

        label_dict,label_size=self.label_format()
        traindata_labels = []
        
        rowList=feature_vectors.collect()
        
        schemaLabel=[0]*label_size
        for layer1 in label_dict:
            for layer2 in label_dict[layer1]:
                schemaLabel[int(label_dict[layer1][layer2])]=str(layer1)+'_withValue_'+str(layer2)
        
        for c in rowList:
            index,column=self.cell2index(c)
            traindata_labels.append(self.make_label_array(index,column))
            
        # put features vectors in train data

        
        result=spark_session.createDataFrame(traindata_labels,schemaLabel)

        
        return result
    


   ##########*** Supplementary Method ***#######
    
    #This method make label mapping format
        
    def label_format(self):
        num_of_rows=self.index2list()
        num_of_coulumns=self.dataset.columns
    
        dictionary={}
        label_size=0
        for j in num_of_coulumns:
            if j != 'index':
                temp=[]
                v={}
                for i in num_of_rows:
                    pp=self.indexValue(i, j)
                    if pp not in temp:
                        temp.append(pp)
                for i in range(0,len(temp)):
                    v.update({temp[i]:label_size})
                    label_size+=1
        
                    dictionary.update({j : v})
    
        return dictionary,label_size
    
    def make_label_array(self,row,column):
        
        dictionary,label_size= self.label_format()
        
        one_hot_label=[0]*label_size
        ind=dictionary[column][self.indexValue(row, column)]
        one_hot_label[ind]=1
    
        return one_hot_label
    
    def indexValue(self,ind,attr):
        """
        Returns the value of cell with index=ind and column=attr
        :type ind: int
        :type attr: string
        :rtype: list[list[None]]
        """
        row=self.dataset.filter(self.dataset['index']==ind).collect()
        row2dict=row[0].asDict()
        
        return row2dict[attr]
        
    def index2list(self):        
        """
        Returns list of indices
        :rtype: list[string]
        """
        li_tmp=self.dataset.select('index').collect()
        
        return [i.asDict()['index'] for i in li_tmp ] 
    
    def cell2index(self,row):
        tmp=row['cell']
        index=tmp['_1']
        column=tmp['_2']
        return index,column     

# data = [
#     [2, 3, 'k'],
#     [1, 3, 'k'],
#     [1, 3, 'k'],
#     [2, 2, 'l'],
#     [2, 4, 'l'],
#     [2, 4, 'l'],
#     [3, 2, 'f']
# ]
# df = pd.DataFrame(data, columns=['a', 'b', 'c'])
# dcCode=['a,0,b,3' , 'c,0,a,3']
# noisy_cells=[(0,'a'),(3,'b')]
# 
# d=HolocleanData(df, noisy_cells, dcCode)
# print(d.cell_to_feature_vector(0, 'b'))

    
