class anti_Wrapper:
	def __init__(self,dataengine,dataset):
		"""TODO.
		Parameters
		--------
		parameter: dataengine,dataset
		"""
		self.dataengine=dataengine
		self.dataset=dataset
		dataframe1=self.dataengine._table_to_dataframe("Feature",self.dataset)
		dataframe2=self.dataengine._table_to_dataframe("Variable",self.dataset)
		variable=[]
		for c in dataframe2.collect():
			if [c[1],c[2]] not in variable:
				variable.append([c[1],c[2]])

		factor=[]
		edge=set()
		factor_index=0
		print variable
		raw_input("asd")
		for c in dataframe1.collect():
			edge.add((variable.index([str(c[1]),c[2]]),factor_index))
			factor_index=factor_index+1
		print edge
		#print variable
			
	
