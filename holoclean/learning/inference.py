import numpy as np

class inference:	
	"""TODO:
    	 This class create the probability table and finds the accuracy of our program
    	"""

	def __init__(self, dataengine,dataset, spark_session):
		"""
		This constructor gets as arguments the dataengine,dataset and spark_session from the Session class
		"""
		self.dataengine = dataengine
		self.dataset = dataset
		self.spark_session = spark_session

	def learning(self):
		"""
		 	To do: creates the probability table for our dataset and checks the accuracy
       		"""
		#query to create the probability table
		query_probability = "CREATE TABLE " +self.dataset.table_specific_name('Probabilities')+" AS ("  
		
		#The attributes of the probability table index,attribute,assigned value, probability for each cell
		query_probability=query_probability +" select table1.rv_attr,table1.rv_index,assigned_val, sum_probabilities/total_sum as probability from" 
		
		#first table of the query where get the sum of all the probabilities for each distinct group of rv_attr,rv_index		
		query_probability=query_probability +" (select sum(sum_probabilities) as total_sum , rv_index,rv_attr from (select exp((-1)*sum(weight_val)) as sum_probabilities ,rv_attr , rv_index,assigned_val from "+self.dataset.table_specific_name('Weights')+" as table1, "+ self.dataset.table_specific_name('Feature') +" as table2 where table1.weight_id=table2.weight_id group by rv_attr,rv_index,assigned_val) as table1 group by rv_attr,rv_index)as table2 ,"
		
		#second table of the query where get the probabilities for each distinct group of rv_attr,rv_index,assigned_val
		query_probability=query_probability+"(select exp((-1)*sum(weight_val)) as sum_probabilities ,rv_attr , rv_index,assigned_val from "+self.dataset.table_specific_name('Weights')+" as table1, "+ self.dataset.table_specific_name('Feature') +" as table2 where table1.weight_id=table2.weight_id group by rv_attr,rv_index,assigned_val) as table1" 
			

		query_probability=query_probability+" where table1.rv_index=table2.rv_index and table1.rv_attr=table2.rv_attr) ; "
		self.dataengine.query(query_probability )

		#query to find the repair for each cell
		query="CREATE TABLE "+self.dataset.table_specific_name('Final')+" AS (" + " select table1.rv_index, table1.rv_attr, assigned_val from (select max(probability) as max1 ,rv_index , rv_attr   from "+self.dataset.table_specific_name('Probabilities')+" group by rv_attr,rv_index) as table1 , "+self.dataset.table_specific_name('Probabilities')+" as table2  where table1.rv_index=table2.rv_index and table1.rv_attr=table2.rv_attr and max1=table2.probability);"
        	self.dataengine.query(query)

		#we get the number of repairs form the dont know cells		
		dataframe1=self.dataengine._table_to_dataframe("C_dk",self.dataset)
		number_of_repairs=dataframe1.count()

		#we get the number of incorrect repairs by comparing the repairs to the correct table
		df1=self.dataengine._table_to_dataframe("Final",self.dataset)
		df2=self.dataengine._table_to_dataframe("Correct",self.dataset)		
		incorrect_repairs=df1.subtract(df2).count()
		
		#we find the accuracy
		accuracy=(1.0)*(number_of_repairs-incorrect_repairs)/number_of_repairs
		return accuracy
	
