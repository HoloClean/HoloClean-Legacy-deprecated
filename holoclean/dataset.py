#!/user/bin/env python
import random
from datetime import datetime

class Dataset:

	attributes = ['id','T', 'C_clean', 'C_dk', 'X', 'D', 'Y',
					'W', 'b', 'Y_prod', 'config']
	
	def __init__(self):
		self.attribute = {}
		for a in Dataset.attributes:
			self.attribute[a] = 0
		self.dataset_id=self.id_generator()
	def setatrribute(self,value,attr):
		self.attribute[attr]=value
		return self.attribute[attr]
	def getattribute(self,attr):
		return self.attribute[attr]
	
	def id_generator(self):
		r=random.seed(datetime.now())
		return str(random.random())[2:]

