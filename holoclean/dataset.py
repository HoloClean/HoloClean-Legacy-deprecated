#!/user/bin/env python
import random
from datetime import datetime

class Dataset:

	attributes = ['id','T', 'C_clean', 'C_dk', 'X', 'D', 'Y',
					'W', 'b', 'Y_prod', 'config']
	table_name=['']*len(attributes)
	def __init__(self):
		self.attribute = {}
		for a in Dataset.attributes:
			self.attribute[a] = 0
		self.dataset_id=self.id_generator()
		for i in range(1,len(self.attributes)):
			self.table_name[i]=self.dataset_id+'_'+self.attributes[i]
	def getattribute(self,attr):
		return self.table_name[attr]
	
	def id_generator(self):
		"""This function create 
		a random id from the system time
		"""	
		r=random.seed(datetime.now())
		return str(random.random())[2:]

