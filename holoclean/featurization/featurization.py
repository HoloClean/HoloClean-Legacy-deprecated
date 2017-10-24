import sys
sys.path.append('../../')
from holoclean.utils.dcparser import DCParser

class Featurizer:
    """TODO.
    Creates the table for the featurization
    """
    
    def __init__(self,denial_constraints,dataengine):
	"""TODO.
	Parameters
	--------
	parameter: denial_constraints,dataengine
	"""
        self.denial_constraints=denial_constraints
        self.dataengine=dataengine
        
    #Internal Methods    
    def _query_for_featurization_of_dc(self,table_featurizer,possible_table_name,table_name):
        
        """TODO: create a query for the featurization table for the dc """
        
        dcp=DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        new_dc=self._create_new_dc()
        table_featurizer='CREATE TABLE '+self.dataengine.dataset.spec_tb_name('dc_f1')+' AS (select * from ( '
        for index_dc in range (0, len(dc_sql_parts)):
            new_condition=new_dc[index_dc ]
            dc="',"+dc_sql_parts[index_dc ]+"'"
            if index_dc ==0:
                table_featurizer+="""(SELECT distinct    table1.index as rv_index,table3.attr_name as rv_attr, table3.attr_val as assigned_val, concat ( table2.index,"""+ dc+ """) as feature,'FD' AS TYPE ,'       ' as weight_id  from """+ table_name +""" as table2, """+ table_name+ """ as table1, """+ possible_table_name+ """ as table3 where ("""+new_condition+""" AND (table1.index<>table2.index) ) )"""
            else:
                #if you have more than one dc
                table_featurizer+=""" UNION SELECT distinct    table1.index as rv_index,table3.attr_name as rv_attr, table3.attr_val as assigned_val, concat ( table2.index,"""+ dc+ """) as feature,'FD' AS TYPE ,'           ' as weight_id  from """+ table_name +""" as table2, """+ table_name+ """ as table1, """+ possible_table_name+ """ as table3 where ("""+new_condition+""" AND (table1.index<>table2.index) )"""
        return table_featurizer
    
    def _query_for_featurization_of_init(self,table_featurizer,possible_table_name,table_name):
        
        """TODO: create a query for the featurization table for the initial values """
        
        table_attribute_string=self.dataengine.get_schema('T')
        table_attribute=table_attribute_string.split(',')
        for attribute in table_attribute:
            if attribute !="index":
                str_attribute="'"+attribute +"'"
                table_attribute="table1."+attribute 
                condition="table3.attr_name="+str_attribute+" and table1.index=table3.tid"
                table_featurizer+=""" union (SELECT distinct    table3.tid as rv_index,table3.attr_name as rv_attr, table3.attr_val as assigned_val, concat ( 'INIT=',"""+ table_attribute + """) as feature,'init' AS TYPE,'      ' as weight_id   from """+ table_name+ """ as table1, """+ possible_table_name+ """ as table3 where ("""+condition+"""  ) )"""
        return table_featurizer

    def _query_for_featurization_of_cooncur(self,table_featurizer,possible_table_name,table_name):
        
        """TODO: create a query for the featurization table for the co-occurance values  """
        
        table_featurizer+=""" union (SELECT distinct table3.tid as rv_index,table3.attr_name as rv_attr, table3.attr_val as assigned_val, concat (table1.attr_name,'=',table1.attr_val ) as feature,'concur' AS TYPE,'        ' as weight_id  from """+ possible_table_name+ """ as table1, """+ possible_table_name+ """ as table3  where (table1.attr_name<>table3.attr_name))"""
        table_featurizer+=""")as table1)order by rv_index,rv_attr,feature;"""    
        return table_featurizer
    
    def _add_weights(self):
        
        """TODO: updates the values of weights for the featurization table """
        
        dataframe=self.dataengine.get_table_spark("dc_f1")
        d=dataframe.columns
        groups=[]
        for c in dataframe.collect():
            temp=[c['rv_index'],c['rv_attr'],c['feature']]
            if temp not in groups:
                groups.append(temp)
        query="UPDATE "+self.dataengine.dataset.spec_tb_name('dc_f1')+" SET weight_id= CASE"
        for weight_id in range(0,len(groups)):
            query+=" WHEN  rv_index='"+groups[weight_id][0]+"'and rv_attr='"+groups[weight_id][1]+"'and feature='"+groups[weight_id][2]+"' THEN "+ str(weight_id)
        query+=" END;"
        self.dataengine.query(query)
        
    def _create_new_dc(self):
        
        """for each dc we change the predicates, and return the new type of dc """
       
        table_attribute_string=self.dataengine.get_schema('T')
        attributes=table_attribute_string.split(',')
        dcp=DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        new_dcs=[]
        dc_id=0
        for c in dc_sql_parts:
            list_preds,type_list=self._find_predicates(c)
        new_dcs.append(self._change_predicates_for_query(list_preds,attributes))
        return new_dcs

    
    def _change_predicates_for_query(self,list_preds,    attributes):
        
        """For each predicats we change it to form that we need for the query to create the featurization table"""
        
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
                            new_pred="table3.attr_name= '"+components_preds[p]+"' AND " + "table3.attr_val"+operation+left_component[1]+"."+components_preds[p]
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


    
    #Setters
    def create_possible_table_value(self):
        
        """TODO: creates the table for all possible values"""
        
        db_name=self.dataengine.dbname
        table_name=self.dataengine.dataset.spec_tb_name('T')
        table_attribute_string=self.dataengine.get_schema('T')
        table_attribute=table_attribute_string.split(',')
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        cell_possible_table='CREATE TABLE '+db_name+'.value_possible1_'+table_name+' AS'
        instance1=table_name+'1'
        instance2=table_name+'2'
        index_name=instance1+'.index'
        for attr in table_attribute:
            if attr != 'index':
                cell_possible_table+=' SELECT '+index_name+' as tid,IF('+index_name+' IS NOT NULL,"'+attr+'","Bad") as attr_name,'+instance2+'.'+attr+' as attr_val FROM '+table_name+' '+instance1+','+table_name+' '+instance2+' UNION '
        cell_possible_table=cell_possible_table[:-6]
        cell_possible_table+=';'
        cursor.execute(cell_possible_table)
	return db_name+'.value_possible1_'+table_name

    
    def create_featurization_table(self):
        
        """TODO: creates the table for the featurization by combyning queries"""

        cursor = self.dataengine.data_engine.raw_connection().cursor()
        possible_table_name=self.create_possible_table_value()
        table_name=self.dataengine.dataset.spec_tb_name('T')
        table_featurizer='CREATE TABLE '+self.dataengine.dataset.spec_tb_name('dc_f1')+' AS (select * from ( '
        table_featurizer=self._query_for_featurization_of_dc(table_featurizer,possible_table_name,table_name)
        table_featurizer=self._query_for_featurization_of_init(table_featurizer,possible_table_name,table_name)
        table_featurizer=self._query_for_featurization_of_cooncur(table_featurizer,possible_table_name,table_name)
        cursor.execute(table_featurizer)
        self._add_weights()


   
    #Methods
       
    def _find_predicates(self,cond):

	"""TODO: finds the predicates of dc"""

        list_preds=cond.split(' AND ')
        type_list=[]
        for p in list_preds:
            type_list.append(self._find_pred_type(p))
        return list_preds,type_list 
 
    def _find_pred_type(self,pred):
	"""finds the type of a predicate"""
        typep=0
        if 'table1' in pred:
            typep+=1
        if 'table2' in pred:
            typep+=1
        if 'table3' in pred:
            typep+=1
        return typep  

   

