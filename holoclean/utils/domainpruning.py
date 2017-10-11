import numpy as np
import copy

class DomainPruning:
        
    selection_threshold=0.5 #preset value for pruning domain value
    
    def __init__(self,data_dataframe,c_dk_dataframe):
        self.data_dataframe=data_dataframe
        self.c_dk_dataframe=c_dk_dataframe
    
    def candidate_values(self):
        """
        This method return active domain of given noisy cells
        :type noisy_data: list[(int,string)]
        :type data_dataframe: pd.DataFrame
        :type threshold: float
        :rtype: list[set[string]]
        """
        repair_candidates=[]
    
        for c in self.c_dk_dataframe.collect(): #Calculate candidate value for each noisy cell
            # Initialize repair candidates
            repair_candidate = set()
            c_row = c['ind']
            c_attr = c['attr']
            domain_c_attr = self.attribute_active_domain(c_attr)
            for attr in list(self.data_dataframe.columns):
                if attr != c_attr: #goes over all other attribute except noisy cell attribute
                    v_attr = self.index_value(c_row, attr)
                    for c_v in domain_c_attr: # for each value in the domain we calculate the p value
                        # Calculate the Pr(v_c | v_attr)
                        
                        num_v_attr=len(self.data_dataframe.filter(self.data_dataframe[attr]==v_attr).collect())    
                        cooc=len(self.data_dataframe.filter(self.data_dataframe[c_attr]==c_v).filter(self.data_dataframe[attr]==v_attr).collect())
                            
                        pr = float(cooc) / num_v_attr
                        if pr >= self.selection_threshold:
                            repair_candidate.add(c_v)
            repair_candidates.append(list(repair_candidate))
        return repair_candidates
    
    #This function get an attribute and return its active domain
    
    def attribute_active_domain(self,attr):
        """
        Returns the full domain of an attr
        :type attr: string
        :rtype: list[string]
        """
        domain = set()
        tmp=self.data_dataframe.select(attr).collect()
        for v in range(0,len(tmp)):
            domain.add(tmp[v].asDict()[attr])
        return list(domain) 
    #By using this function we can change the domain pruning threshold
    
    def set_threshold(self,new_threshold):
        """
        This method set the object threshold to new value
        :type new_threshold: float
        """
        self.selection_threshold=new_threshold
    
    #This function create not allowed index by getting noisy cells
   
    def unallowable_index_list(self):
        """
        This method return indices that a noisy cell appeared 
        :rtype: list[None]
        """
        result=set()
        for c in self.c_dk_dataframe.collect():
            result.add(c['ind'])
        
        return  list(result) #Noisy attributes 
   
    
    #    This function calculate and return a list of lists that specifies valid value for each attribute in order they appeared in dataframe\
    
    def index_value(self,ind,attr):
        """         
        Returns the value of cell with index=ind and column=attr
        :type ind: int
        :type attr: string
        :rtype: list[list[None]] 
        """
        row=self.data_dataframe.filter(self.data_dataframe['index']==ind).collect()
        row2dict=row[0].asDict()
        
        return row2dict[attr]
        
    def allowable_doamin_value(self,spark_session):
        
        #This part calculate the over all domain
        new_domain={}
        attributes=self.data_dataframe.columns
        for a in attributes:
            if a != 'index':        
                new_domain.update({a:np.unique([self.index_value(k, a) for k in range(1,self.data_dataframe.count()+1)]).tolist()})        
        #This part calculate value for noisy cells
        old_domain=copy.deepcopy(new_domain)
        allowed_value_for_noisy=self.candidate_values()
        #Attribute that are noisy
        tmp=set()
        for c in self.c_dk_dataframe.collect():
            tmp.add(c['attr'])
        noisy_attribute=list(tmp)
        pruned_domain={a:[] for a in noisy_attribute}
        noisy_attribute_inorder=[c['attr'] for c in self.c_dk_dataframe.collect()]
        #Calculate
        index_over_noisy_domain=0
        for k in noisy_attribute_inorder :
            pruned_domain.update({k:np.unique(pruned_domain[k]+allowed_value_for_noisy[index_over_noisy_domain]).tolist()})
            index_over_noisy_domain+=1
        for a in pruned_domain:
            new_domain.update({a:pruned_domain[a]})
        
        new_domain_list=[]
        for attr,value_list in new_domain.iteritems():
            for i in value_list:
                new_domain_list.append([attr,i])
        schema=['attr','value']
        new_df = spark_session.createDataFrame(new_domain_list,schema)
    
        return new_df 
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
            
            if index not in unallowed_index and self.index_value(index, column) in allowed_domain_value[column]:         
                data.append(row)
        result_dataframe=spark_session.createDataFrame(data,joint_table.columns)        
        return result_dataframe
    
    def cell2index(self,row):
        tmp=row['cell']
        index=tmp['_1']
        column=tmp['_2']
        return index,column
    
    