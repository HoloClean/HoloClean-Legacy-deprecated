import sys
sys.path.append('../../')
from holoclean.utils import dcparser
import pyspark as ps

class DCFeaturizer:
    
    #########################################################
    ################# Initialize ############################ 
    #########################################################
    
    #Set of operations that can appear in the denial constraints 

    
    #For each QuantativeStatisticsFeaturize , data_dataframe and noisy cells are needed
    def __init__(self,data_dataframe,denial_constraints):
        self.data_dataframe=data_dataframe
        self.denial_constraints=denial_constraints
   
 
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
   
    #########################################################
    ########## Make feature for multiple tuples ############# 
    #########################################################
    
    #@@@@@@@@ Main Method @@@@@@@@@@#
    
    def featurize(self,noviolations,spark_session):
        temp_dataset=self.data_dataframe
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
                    col_changed=(dc_count) * self.data_dataframe.count() + indexCol.index(col_tuple) + 1
                    if self.inclusion(curr_tuple_attribute,dc_count):                       
                        if int(curr_tuple_index) == int(row_tuple):
                            data[tu_count][col_changed] = 1
                    else:
                        data[tu_count][col_changed] = 0

        col_names=['cell']+[str(i) for i in range(1,temp_dataset.count()*len(self.denial_constraints)+1)]

        new_df = spark_session.createDataFrame(data,col_names)
        
        return(new_df)  
   
    
    #$$$$$$$$$$$$$$$$ Not Main $$$$$$$$$$$$$$
    
    
    def pre_features(self,spark_session):
        data=[]
        index_id=self.index2list()
        for i in index_id:
            for a in self.data_dataframe.columns:
                if a!='index':
                    act_dom=self.attribute_active_domain(a)
                    for val in act_dom:
                        for dc in self.denial_constraints:
                            for j in index_id:
                                if self.violate(i,a,val,j,dc,spark_session):
                                    data.append([str('t_'+i+'.'+a),val,dc,j])
        col_names=['rv','assigned','dc','tup_id']                            
        new_df = spark_session.createDataFrame(data,col_names)
        
        return new_df
    
    """Supplementary methods"""
    
    def violate(self,cell_index,cell_attr,value,second_tuple_index,dc,spark_session):
        if cell_attr not in dc :
            return False
        else:
            dc_index=self.denial_constraints.index(dc)
            dcp=dcparser.DCParser(self.denial_constraints)
            dcSql,usedOperations=dcp.dc2SqlCondition()
                   
            standard_dc=dcSql[dc_index]
            op_dc=usedOperations[dc_index]
            self.data_dataframe.createOrReplaceTempView("df")
            q="SELECT * FROM df WHERE( index = '"+str(cell_index) +"' OR index='"+str(second_tuple_index)+"')"
            df_q_result= spark_session.sql(q)
            rows=df_q_result.collect()
            new_dic=[]
            for r in rows :
                tmp=r.asDict()
                if tmp['index']==str(cell_index):
                    tmp[cell_attr]=value
                new_dic.append(tmp)
            new_df = spark_session.createDataFrame(new_dic)
            new_df.createOrReplaceTempView("dfn")
            sql_q=dcp.make_and_condition(conditionInd = 'all')[dc_index]
            q="SELECT table1.index as indexT1,table2.index as indexT2 FROM dfn table1,dfn table2 WHERE ("+ sql_q +")"
            
            result=spark_session.sql(q)
            if result.count()== 0:
                return False
            else:
                return True
            
            

                         
    
    def attribute_active_domain(self,attribute):
            
        """
        Returns the full domain of an attribute
        :type attr: string
        :rtype: list[string]
        """
        domain = set()
        tmp=self.data_dataframe.select(attribute).collect()
        for v in range(0,len(tmp)):
            domain.add(tmp[v].asDict()[attribute])
        return list(domain) 
    
    
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
    
    # This function make feature vector for a cell with index i  j in the data_dataframe
    
    
    def make_featurvector(self, index, attribute ,parsedDCs,spak_session):
        numOfTuple = self.data_dataframe.count()
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
        dfLeft=self.data_dataframe.filter(self.data_dataframe['index']==indexTuple1)
        dfRight=self.data_dataframe.filter(self.data_dataframe['index']==indexTuple2)
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
        li_tmp=self.data_dataframe.select('index').collect()
        
        return [i.asDict()['index'] for i in li_tmp ]

    
    def indexValue(self,ind,attr):

        """
        Returns the value of cell with index=ind and column=attr
        :type ind: int
        :type attr: string
        :rtype: list[list[None]]
        """
        row=self.data_dataframe.filter(self.data_dataframe['index']==ind).collect()
        row2dict=row[0].asDict()
        
        return row2dict[attr]    
    
    
  
    