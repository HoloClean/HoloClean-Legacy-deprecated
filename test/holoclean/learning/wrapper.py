import numpy as np
from numbskull.numbskulltypes import *


class Wrapper:

	def __init__(self,dataengine,dataset):
		"""TODO.
		Parameters
		--------
		parameter: denial_constraints,dataengine
		"""
		self.dataset=dataset
		self.dataengine=dataengine
		self._make_dictionary()

	def _make_dictionary(self):

		domain_dataframe=self.dataengine._table_to_dataframe("Domain",self.dataset)
		temp = domain_dataframe.select("attr_name", "attr_val").collect()
		attribute = ""
		id = 0
		self.dictionary = {}
		secDic = {}
		dic_list=[]
		for row in temp:
			dic_list.append(row.asDict())
		attr_set=set()
		for element in dic_list:
			attr_set.add(element["attr_name"])
		attr_set=list(attr_set)
		result={}
		for a in attr_set:
			dict={}
			result.update({a:dict})
		id=[0]*len(attr_set)
		for element in dic_list:
			result[element["attr_name"]].update({element["attr_val"]:id[attr_set.index(element["attr_name"])]})
			id[attr_set.index(element["attr_name"])]+=1

		# for row in temp:
		# 	tempdictionary = row.asDict()
		# 	if attribute == tempdictionary["attr_name"]:
		# 		secDic.update({tempdictionary["attr_val"]: id})
		# 		id = id + 1
		# 	else:
		# 		secDic.update({tempdictionary["attr_val"]: id})
		# 		id = 0
		# 		secDic = {}
		# 		attribute = tempdictionary["attr_name"]
        #
		# 	self.dictionary.update({attribute: secDic})
		self.dictionary=result

	def set_weight(self):
		"""
        	This method creates a query for weight table for the factor"
	
       		"""
		
		mysql_query="Create TABLE "+self.dataset.table_specific_name('Weights')+" as (select weight_id,'1' as Is_fixed,'1' as init_val from "+ self.dataset.table_specific_name('Feature') +" where TYPE='init' group by weight_id  ) union (select weight_id,'0' as Is_fixed,'0' as init_val from "+ self.dataset.table_specific_name('Feature') +" where TYPE!='init' group by weight_id  ); alter table "+self.dataset.table_specific_name('Weights')+" modify weight_id,Is_fixed,init_val INTEGER ;alter table "+self.dataset.table_specific_name('Weights')+" order by weight_id;"
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
		mysql_query=mysql_query+ "(select NULL as factor_to_var_index, variable_index as vid,attr_val,table1.attr_name from  "+self.dataset.table_specific_name('Possible_values')+" as table1, "+self.dataset.table_specific_name('Variable')+" as table2 where  table1.tid=rv_ind and   table1.attr_name=table2.rv_attr)order by vid;ALTER TABLE "+self.dataset.table_specific_name('Factor_to_var')+" MODIFY factor_to_var_index INT AUTO_INCREMENT PRIMARY KEY;"
		self.dataengine.query(mysql_query)

	def set_factor(self):	
		"""
        	This method creates a query for factor table for the factor"
	
       		"""
		mysql_query='CREATE TABLE '+self.dataset.table_specific_name('Factor')+' AS'
		mysql_query=mysql_query+ "(select distinct NULL as factor_index, '4' as FactorFunction, table1.weight_id as weightID, '1' as Feature_Value, '1' as arity, table3.factor_to_var_index as ftv_offest from "+self.dataset.table_specific_name('Feature')+" as table1,"+self.dataset.table_specific_name('Variable')+" as table2,"+self.dataset.table_specific_name('Factor_to_var')+" as table3 where  table1.rv_index=table2.rv_ind and table1.rv_attr= table2.rv_attr and table3.vid=table2.variable_index and table3.attr_val=table1.assigned_val );ALTER TABLE "+self.dataset.table_specific_name('Factor')+" MODIFY factor_index INT AUTO_INCREMENT PRIMARY KEY;"
		self.dataengine.query(mysql_query)

	def spark_list_weight(self):
		weight_dataframe=self.dataengine._table_to_dataframe("Weights",self.dataset)
		temp=weight_dataframe.select("Is_fixed","init_val").collect()
		weight_list=[]
		for row in temp:
			tempdictionary=row.asDict()
			weight_list.append([(tempdictionary["Is_fixed"]),tempdictionary["init_val"]])
		weight = np.zeros(len(weight_list), Weight)

		count=0
		for w in weight:
			w["isFixed"]=weight_list[count][0]
			w["initialValue"]=weight_list[count][1]
			count+=1

		return weight

	def spark_list_variable(self):
		variable_dataframe=self.dataengine._table_to_dataframe("Variable",self.dataset)




		temp=variable_dataframe.select("rv_attr","is_Evidence","initial_value","Datatype","Cardinality","vtf_offset").collect()
		variable_list=[]
		for row in temp:
			tempdictionary=row.asDict()
			if int(tempdictionary["is_Evidence"])==0:
				variable_list.append([np.int8(int(tempdictionary["is_Evidence"])),np.int64((int(0))),np.int16(int(tempdictionary["Datatype"])),np.int64(int(tempdictionary["Cardinality"])),np.int64(int(tempdictionary["vtf_offset"]))])	
			else:
				variable_list.append([np.int8(int(tempdictionary["is_Evidence"])),np.int64((int(self.dictionary[tempdictionary["rv_attr"]][tempdictionary["initial_value"]]))),np.int16(int(tempdictionary["Datatype"])),np.int64(int(tempdictionary["Cardinality"])),np.int64(int(tempdictionary["vtf_offset"]))])
			variable=np.zeros(len(variable_list), Variable)
		count=0
		for v in variable:
			v["isEvidence"]=variable_list[count][0]
			v["initialValue"] = variable_list[count][1]
			v["dataType"] = variable_list[count][2]
			v["cardinality"] = variable_list[count][3]
			v["vtf_offset"] = variable_list[count][4]
			count+=1
		return variable

	def spark_list_Factor_to_var(self):
		Factor_to_var_dataframe=self.dataengine._table_to_dataframe("Factor_to_var",self.dataset)
		Factor_to_var_dataframe.sort("vid")
		temp=Factor_to_var_dataframe.select("vid","attr_val","attr_name").collect()
		Factor_to_var_list=[]
		
		for row in temp:
			tempdictionary=row.asDict()
			Factor_to_var_list.append([np.int64(tempdictionary["vid"]),np.int64(self.dictionary[tempdictionary["attr_name"]][tempdictionary["attr_val"]])])
		fmap=np.zeros(len(Factor_to_var_list), FactorToVar)
		count=0
		for f in fmap:
			f["vid"]=Factor_to_var_list[count][0]-1
			f["dense_equal_to"]=Factor_to_var_list[count][1]
			count+=1
		return fmap

	def spark_list_Factor(self):
		Factor_dataframe=self.dataengine._table_to_dataframe("Factor",self.dataset)
		temp=Factor_dataframe.select("FactorFunction","weightID","Feature_Value","arity","ftv_offest").collect()
		Factor_list=[]
		
		for row in temp:
			tempdictionary=row.asDict()
			Factor_list.append([np.int16(tempdictionary["FactorFunction"]),np.int64(tempdictionary["weightID"]),np.float64(tempdictionary["Feature_Value"]),np.int64(tempdictionary["arity"]),np.int64((int(tempdictionary["ftv_offest"])-1))])

		factor=np.zeros(len(Factor_list), Factor)

		count=0
		for f in factor:
			f["factorFunction"]=Factor_list[count][0]
			f["weightId"] = Factor_list[count][1]
			f["featureValue"] = Factor_list[count][2]
			f["arity"] = Factor_list[count][3]
			f["ftv_offset"] = Factor_list[count][4]
			count+=1


		return factor

	def create_edge(self,Factor_list):
		edges=len(Factor_list)
		return edges

	def create_mask(self,variable_list):
		mask=np.zeros(len(variable_list), np.bool)
		return mask

