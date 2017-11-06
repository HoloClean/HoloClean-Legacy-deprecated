from dcparser import DCParser

class Featurizer:
    """TODO.
    Creates the table for the featurization
    """
    
    def __init__(self,denial_constraints,dataengine,dataset):
	"""TODO.
	Parameters
	--------
	parameter: denial_constraints,dataengine
	"""
        self.denial_constraints=denial_constraints
        self.dataengine=dataengine
	self.dataset=dataset
        
    #Internal Methods    
    def _query_for_featurization_of_dc(self,query_for_featurization,possible_table_name,table_name):
        
	"""
        This method creates a query for the featurization table for the dc"
	Parameters
	--------
	possible_table_name: the name of table with all the possible values
	table_name: the name of the initial table
	query_for_featurization: the initial query that we will update
	
        """
        
        dcp=DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        new_dc=self._create_new_dc()
        for index_dc in range (0, len(dc_sql_parts)):
            new_condition=new_dc[index_dc ]
            dc="',"+dc_sql_parts[index_dc ]+"'"
            if index_dc ==0:
                query_for_featurization+="""(SELECT distinct    table1.index as rv_index,possible_table.attr_name as rv_attr, possible_table.attr_val as assigned_val, concat ( table2.index,"""+ dc+ """) as feature,'FD' AS TYPE ,'       ' as weight_id  from """+ table_name +""" as table2, """+ table_name+ """ as table1, """+ possible_table_name+ """ as possible_table where ("""+new_condition+""" AND (table1.index<>table2.index) ) )"""
            else:
                #if you have more than one dc
                query_for_featurization+=""" UNION SELECT distinct    table1.index as rv_index,possible_table.attr_name as rv_attr, possible_table.attr_val as assigned_val, concat ( table2.index,"""+ dc+ """) as feature,'FD' AS TYPE ,'           ' as weight_id  from """+ table_name +""" as table2, """+ table_name+ """ as table1, """+ possible_table_name+ """ as possible_table where ("""+new_condition+""" AND (table1.index<>table2.index) )"""
        return query_for_featurization
    
    def _query_for_featurization_of_init(self,query_for_featurization,possible_table_name,table_name):
        

	"""
        This method creates a query for the featurization table for the initial values"
	Parameters
	--------
	possible_table_name: the name of table with all the possible values
	table_name: the name of the initial table
	query_for_featurization: the initial query that we will update
	
        """
        
        dataframe=self.dataengine._table_to_dataframe("Init",self.dataset)
        table_attribute=dataframe.columns

        for attribute in table_attribute:
            if attribute !="index":
                str_attribute="'"+attribute +"'"
                table_attribute="table1."+attribute 
                condition="possible_table.attr_name="+str_attribute+" and table1.index=possible_table.tid"
                query_for_featurization+=""" union (SELECT distinct    possible_table.tid as rv_index,possible_table.attr_name as rv_attr, possible_table.attr_val as assigned_val, concat ( 'INIT=',"""+ table_attribute + """) as feature,'init' AS TYPE,'      ' as weight_id   from """+ table_name+ """ as table1, """+ possible_table_name+ """ as possible_table where ("""+condition+"""  ) )"""
        return query_for_featurization

    def _query_for_featurization_of_cooccur(self,query_for_featurization,possible_table_name,table_name):

	"""
        This method creates a query for the featurization table for the co-occurance values"
	Parameters
	--------
	possible_table_name: the name of table with all the possible values
	table_name: the name of the initial table
	query_for_featurization: the initial query that we will update
	
        """
        
        query_for_featurization+=""" union (SELECT distinct possible_table.tid as rv_index,possible_table.attr_name as rv_attr, possible_table.attr_val as assigned_val, concat (table1.attr_name,'=',table1.attr_val ) as feature,'concur' AS TYPE,'        ' as weight_id  from """+ possible_table_name+ """ as table1, """+ possible_table_name+ """ as possible_table  where (table1.attr_name<>possible_table.attr_name))"""
            
        return query_for_featurization
    
    def _add_weights(self):
        
	"""
        This method updates the values of weights for the featurization table"
	"""
        
        dataframe=self.dataengine._table_to_dataframe("Feature",self.dataset)
        d=dataframe.columns
        groups=[]
        for c in dataframe.collect():
            temp=[c['rv_index'],c['rv_attr'],c['feature']]
            if temp not in groups:
                groups.append(temp)
        query="UPDATE "+self.dataengine.dataset.spec_tb_name('Feature')+" SET weight_id= CASE"
        for weight_id in range(0,len(groups)):
            query+=" WHEN  rv_index='"+groups[weight_id][0]+"'and rv_attr='"+groups[weight_id][1]+"'and feature='"+groups[weight_id][2]+"' THEN "+ str(weight_id)
        query+=" END;"
        self.dataengine.query(query)

    def _create_new_dc(self):
        
 
	"""
        For each dc we change the predicates, and return the new type of dc 
	"""
       
       	dataframe=self.dataengine._table_to_dataframe("Init",self.dataset)
        attributes=dataframe.columns
        dcp=DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
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
                            new_pred_list.append([new_pred])
                            break
        new_dc=""
        new_dc=new_dc+"("+new_pred_list[0][0]+")"
        for i in (1,len(new_pred_list)-1):
            new_dc=new_dc+" OR "+"("+new_pred_list[i][0]+")"
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
    
    def create_featurization_table(self):
        
	"""
        This method creates the table for the featurization by combining queries
        """
        possible_table_name=self.dataset.spec_tb_name('Domain')
        table_name=self.dataset.spec_tb_name('Init')
        query_for_featurization='CREATE TABLE '+self.dataengine.dataset.spec_tb_name('Feature')+' AS (select * from ( '
        query_for_featurization=self._query_for_featurization_of_dc(query_for_featurization,possible_table_name,table_name)
        query_for_featurization=self._query_for_featurization_of_init(query_for_featurization,possible_table_name,table_name)
        query_for_featurization=self._query_for_featurization_of_cooccur(query_for_featurization,possible_table_name,table_name)
	query_for_featurization+=""")as Feature)order by rv_index,rv_attr,feature;"""
        self.dataengine.query(query_for_featurization)
        self._add_weights()

   

