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
		
		mysql_query="Create TABLE "+self.dataset.table_specific_name('Weights')+" as (select weight_id,'0' as Is_fixed,'0' as init_val from "+ self.dataset.table_specific_name('Feature') +" group by weight_id );"
		self.dataengine.query(mysql_query)

	def set_variable(self):
		"""
        	This method creates a query for variable table for the factor"
	
       		"""

		mysql_query='CREATE TABLE '+self.dataset.table_specific_name('Variable')+' AS'
		mysql_query=mysql_query+"(select distinct NULL as variable_index, table1.tid as rv_ind,table1.attr_name as rv_attr,'0' as is_Evidence,0 as initial_value,'1' as Datatype,count1 as Cardinality, NULL as vtf_offset  from "+self.dataset.table_specific_name('Possible_values')+" as table1,"+self.dataset.table_specific_name('C_dk')+" as table2,(select count(*) as count1,attr_name from "+self.dataset.table_specific_name('Domain')+" group by(attr_name)) as counting where table1.tid=table2.ind and table1.attr_name=table2.attr and counting.attr_name=table1.attr_name)"

		table_attribute_string=self.dataengine._get_schema(self.dataset,"Init")
       		attributes=table_attribute_string.split(',')
		for attribute in attributes:
			if attribute !="index":
				mysql_query=mysql_query+"union (select NULL as variable_index, table1.index as rv_ind,table2.attr as rv_attr,'1' as is_Evidence ,"+attribute+" as initial_value,'1' as Datatype, count1 as Cardinality, NULL as vtf_offset  from "+self.dataset.table_specific_name('Init')+" as table1, "+self.dataset.table_specific_name('C_clean')+" as table2, (select count(*) as count1,attr_name from 0909504267486_Domain group by(attr_name)) as counting where table2.ind=table1.index and table2.attr='"+attribute+"' and counting.attr_name='"+attribute+"')"
		mysql_query=mysql_query+";ALTER TABLE "+self.dataset.table_specific_name('Variable')+" MODIFY variable_index INT AUTO_INCREMENT PRIMARY KEY;"
		self.dataengine.query(mysql_query)

	def set_factor(self):	
		"""
        	This method creates a query for factor table for the factor"
	
       		"""
		mysql_query='CREATE TABLE '+self.dataset.table_specific_name('Factor')+' AS'
		mysql_query=mysql_query+ "(select  NULL as variable_index, '4' as activation, table1.weight_id as weightID, '1' as Feature_Value, '1' as rv, NULL ftv_offest from "+self.dataset.table_specific_name('Feature')+" as table1);ALTER TABLE "+self.dataset.table_specific_name('Factor')+" MODIFY variable_index INT AUTO_INCREMENT PRIMARY KEY;"
		self.dataengine.query(mysql_query)
