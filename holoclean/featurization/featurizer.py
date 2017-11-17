from holoclean.utils.dcparser import DCParser

class Featurizer:
    """TODO.
    parent class for all the signals
    """
    
    def __init__(self,denial_constraints,dataengine,dataset):
	"""TODO.
	Parameters
	--------
	parameter: denial_constraints,dataengine,dataset
	"""
        self.denial_constraints=denial_constraints
        self.dataengine=dataengine
	self.dataset=dataset
	self.possible_table_name=self.dataset.table_specific_name('Possible_values')
        self.table_name=self.dataset.table_specific_name('Init')
        
    #Internal Method 
    def _create_new_dc(self):
        
 
	"""
        For each dc we change the predicates, and return the new type of dc 
	"""
       	table_attribute_string=self.dataengine._get_schema(self.dataset,"Init")
        attributes=table_attribute_string.split(',')
        dcp=DCParser(self.denial_constraints)
        dc_sql_parts=dcp.get_anded_string(conditionInd = 'all')
        new_dcs=[]
        dc_id=0
        for c in dc_sql_parts:
            list_preds=self._find_predicates(c)
            new_dcs.append(self._change_predicates_for_query(list_preds,attributes))
        return new_dcs

    
    def _change_predicates_for_query(self,list_preds,    attributes):
        
	"""
        For each predicats we change it to the form that we need for the query to create the featurization table
	Parameters
	--------
	list_preds: a list of all the predicates of a dc
	attributes: a list of attributes of our initial table
        """
        
        operationsArr=['<>' , '<=' ,'>=','=' , '<' , '>']
        new_pred_list=[]
        for i in range(0,len(list_preds)):
            components_preds=list_preds[i].split('.')
            new_pred=""
            for p in (0,len(components_preds)-1):
                if components_preds[p] in attributes:
                    for operation in operationsArr:
                        if operation in components_preds[p-1] :
                            left_component=components_preds[p-1].split(operation)
                            new_pred="possible_table.attr_name= '"+components_preds[p]+"' AND " + "possible_table.attr_val"+operation+left_component[1]+"."+components_preds[p]
                            break
                    for k in range(0,len(list_preds)):
                        if k!=i:
                            new_pred=new_pred+" AND "+list_preds[k]
                            new_pred_list.append(new_pred)
                            break
        new_dc=""
        new_dc=new_dc+"("+new_pred_list[0]+")"
        for i in range (1,len(new_pred_list)):
            new_dc=new_dc+" OR "+"("+new_pred_list[i]+")"
        return new_dc

    def _find_predicates(self,cond):

	"""
        This method finds the predicates of dc"
        :param cond: a denial constrain
        :rtype: list_preds: list of predicates
        """
	

        list_preds=cond.split(' AND ')
        return list_preds


    
    #Setters
    def add_weights(self):
        
	"""
        This method updates the values of weights for the featurization table"
	"""
        
        dataframe=self.dataengine._table_to_dataframe("Feature",self.dataset)
        d=dataframe.columns
        groups=[]
        for c in dataframe.collect():
	    temp=[c['rv_attr'],c['feature']]
            if temp not in groups:
                groups.append(temp)
        query="UPDATE "+self.dataset.table_specific_name('Feature')+" SET weight_id= CASE"
        for weight_id in range(0,len(groups)):
            query+=" WHEN  rv_attr='"+groups[weight_id][0]+"'and feature='"+groups[weight_id][1]+"' THEN "+ str(weight_id)
        query+=" END;"
        self.dataengine.query(query)

class Signal_Init(Featurizer):
	"""TODO.
   	 Signal for initial values
    	"""
	
	def __init__(self,denial_constraints,dataengine,dataset):
		"""TODO.
		Parameters
		--------
		parameter: denial_constraints,dataengine,dataset
		"""
		Featurizer.__init__(self,denial_constraints,dataengine,dataset)

	def get_query(self):

		"""
		This method creates a query for the featurization table for the initial values"
		"""
		
		table_attribute_string=self.dataengine._get_schema(self.dataset,"Init")
		table_attribute=table_attribute_string.split(',')
		query_for_featurization=""
		for attribute in table_attribute:
		    if attribute !="index":
		        str_attribute="'"+attribute +"'"
		        table_attribute="table1."+attribute 
		        condition="possible_table.attr_name="+str_attribute+" and table1.index = possible_table.tid"
		        query_for_featurization+=""" (SELECT distinct NULL as var_index,   possible_table.tid as rv_index,possible_table.attr_name as rv_attr, possible_table.attr_val as assigned_val, concat ( 'INIT=',"""+ table_attribute + """) as feature,'init' AS TYPE,'      ' as weight_id   from """+ self.table_name+ """ as table1, """+ self.possible_table_name+ """ as possible_table where ("""+condition+"""  ) and (possible_table.attr_val=table1."""+attribute +""") ) UNION"""
		query_for_featurization=query_for_featurization[:-5]
		return query_for_featurization

class Signal_cooccur(Featurizer):
	"""TODO.
   	 Signal for cooccurance
    	"""

	def __init__(self,denial_constraints,dataengine,dataset):
		"""TODO.
		Parameters
		--------
		parameter: denial_constraints,dataengine,dataset
		"""
		Featurizer.__init__(self,denial_constraints,dataengine,dataset)
	
	def get_query(self):   
		"""
		This method creates a query for the featurization table for the cooccurances
		"""     
		self.table_name1=self.dataset.table_specific_name('dc_f1')
		query_for_featurization=""" (SELECT distinct  NULL as var_index, possible_table.tid as rv_index,possible_table.attr_name as rv_attr, possible_table.attr_val as assigned_val, concat (table1.attr_name,'=',table1.attr_val ) as feature,'cooccur' AS TYPE,'        ' as weight_id  from """+ self.table_name1 + """ as table1, """+ self.possible_table_name+ """ as possible_table  where (table1.attr_name != possible_table.attr_name and table1.tid = possible_table.tid ))"""
                return query_for_featurization


                return query_for_featurization

class Signal_dc(Featurizer):

	"""TODO.
   	 Signal for dc
    	"""

	def __init__(self,denial_constraints,dataengine,dataset):
		"""TODO.
		Parameters
		--------
		parameter: denial_constraints,dataengine,dataset
		"""
		Featurizer.__init__(self,denial_constraints,dataengine,dataset)	
 	def get_query(self):      
		"""
		This method creates a query for the featurization table for the dc"
		"""
		dcp=DCParser(self.denial_constraints)
		dc_sql_parts=dcp.get_anded_string(conditionInd = 'all')
		new_dc=self._create_new_dc()
		for index_dc in range (0, len(dc_sql_parts)):
		    new_condition=new_dc[index_dc ]
		    dc="',"+dc_sql_parts[index_dc ]+"'"
		    if index_dc ==0:
		        query_for_featurization="""(SELECT distinct  NULL as var_index,  possible_table.tid as rv_index,possible_table.attr_name as rv_attr, possible_table.attr_val as assigned_val, concat ( table2.index,"""+ dc+ """) as feature,'FD' AS TYPE ,'       ' as weight_id  from """+ self.table_name +""" as table2, """+ self.table_name+ """ as table1, """+ self.possible_table_name+ """ as possible_table where (("""+new_condition+""") AND (possible_table.tid != table2.index) ) )"""
		    else:
		        #if you have more than one dc
		        query_for_featurization+=""" UNION (SELECT distinct  NULL as var_index, possible_table.tid as rv_index,possible_table.attr_name as rv_attr, possible_table.attr_val as assigned_val, concat ( table2.index,"""+ dc+ """) as feature,'FD' AS TYPE ,'       ' as weight_id  from """+ self.table_name +""" as table2, """+ self.table_name+ """ as table1, """+ self.possible_table_name+ """ as possible_table where (("""+new_condition+""") AND (possible_table.tid != table2.index) ) )"""
		return query_for_featurization	
	   

