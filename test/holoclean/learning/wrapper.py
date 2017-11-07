#import numbskull
class Wrapper:
	def __init__(self,dataengine,dataset):
		"""TODO.
			Parameters
		--------
		parameter: denial_constraints,dataengine
		"""
		self.dataset=dataset
		self.dataengine=dataengine
	def set_weight(self):
		"""
        	This method creates a query for weight table for the factor"
	
       		"""
		
		mysql_query="Create TABLE "+self.dataset.table_specific_name('Weights')+" as (select weight_id,'1' as Is_fixed,'1' as init_val from "+ self.dataset.table_specific_name('Feature') +" where TYPE='init' group by weight_id  ) union (select weight_id,'0' as Is_fixed,'0' as init_val from "+ self.dataset.table_specific_name('Feature') +" where TYPE!='init' group by weight_id  )"
		self.dataengine.query(mysql_query)

	def set_variable(self):
		"""
        	This method creates a query for variable table for the factor"
	
       		"""

		mysql_query='CREATE TABLE '+self.dataset.table_specific_name('Variable')+' AS'
		mysql_query=mysql_query+"(select distinct NULL as variable_index, table1.tid as rv_ind,table1.attr_name as rv_attr,'0' as is_Evidence,0 as initial_value,'1' as Datatype,count1 as Cardinality, '       ' as vtf_offset  from "+self.dataset.table_specific_name('Possible_values')+" as table1,"+self.dataset.table_specific_name('C_dk')+" as table2,(select count(*) as count1,attr_name from "+self.dataset.table_specific_name('Domain')+" group by(attr_name)) as counting where table1.tid=table2.ind and table1.attr_name=table2.attr and counting.attr_name=table1.attr_name)"

		table_attribute_string=self.dataengine._get_schema(self.dataset,"Init")
       		attributes=table_attribute_string.split(',')
		for attribute in attributes:
			if attribute !="index":
				mysql_query=mysql_query+"union (select NULL as variable_index, table1.index as rv_ind,table2.attr as rv_attr,'1' as is_Evidence ,"+attribute+" as initial_value,'1' as Datatype, count1 as Cardinality, '       ' as vtf_offset  from "+self.dataset.table_specific_name('Init')+" as table1, "+self.dataset.table_specific_name('C_clean')+" as table2, (select count(*) as count1,attr_name from "+self.dataset.table_specific_name('Domain')+" group by(attr_name)) as counting where table2.ind=table1.index and table2.attr='"+attribute+"' and counting.attr_name='"+attribute+"')"
		mysql_query=mysql_query+";ALTER TABLE "+self.dataset.table_specific_name('Variable')+" MODIFY variable_index INT AUTO_INCREMENT PRIMARY KEY;Update "+self.dataset.table_specific_name('Variable')+" SET vtf_offset = (select min(var_index) as offset from "+self.dataset.table_specific_name('Feature')+" as table1 WHERE  "+self.dataset.table_specific_name('Variable')+".rv_ind=table1.rv_index AND  "+self.dataset.table_specific_name('Variable')+".rv_attr= table1.rv_attr  group by rv_index,rv_attr) ;"
		self.dataengine.query(mysql_query)

	def set_Factor_to_Var(self):
		mysql_query='CREATE TABLE '+self.dataset.table_specific_name('Factor_to_var')+' AS'
		mysql_query=mysql_query+ "(select NULL as factor_to_var_index, variable_index as vid,attr_val from  "+self.dataset.table_specific_name('Possible_values')+" as table1, "+self.dataset.table_specific_name('Variable')+" as table2 where  table1.tid=rv_ind and   table1.attr_name=table2.rv_attr);ALTER TABLE "+self.dataset.table_specific_name('Factor_to_var')+" MODIFY factor_to_var_index INT AUTO_INCREMENT PRIMARY KEY;"  
		self.dataengine.query(mysql_query)

	def set_factor(self):	
		"""
        	This method creates a query for factor table for the factor"
	
       		"""
		mysql_query='CREATE TABLE '+self.dataset.table_specific_name('Factor')+' AS'
		mysql_query=mysql_query+ "(select distinct NULL as factor_index, '4' as activation, table1.weight_id as weightID, '1' as Feature_Value, '1' as arity, table3.factor_to_var_index as ftv_offest from "+self.dataset.table_specific_name('Feature')+" as table1,"+self.dataset.table_specific_name('Variable')+" as table2,"+self.dataset.table_specific_name('Factor_to_var')+" as table3 where  table1.rv_index=table2.rv_ind and table1.rv_attr= table2.rv_attr and table3.vid=table2.variable_index and table3.attr_val=table1.assigned_val );ALTER TABLE "+self.dataset.table_specific_name('Factor')+" MODIFY factor_index INT AUTO_INCREMENT PRIMARY KEY;"
		self.dataengine.query(mysql_query)
