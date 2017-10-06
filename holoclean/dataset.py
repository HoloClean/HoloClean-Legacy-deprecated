#!/user/bin/env python
import random
from datetime import datetime

class Dataset:

	attributes = ['id','T', 'C_clean', 'C_dk','dc_f', 'X', 'D', 'Y',
					'W', 'b', 'Y_pred', 'config']
	
	"""
	
	Each element stand for some data that the holoclean needs or create:
	
		id : is the unique id for the dataset and it will be used in registering and retrieving data 
		T : the initial data that get to the database from a file that user give
		C_clean : it is table with index of clean cells 
		C_dk : is table of indices that we don't know they are noisy or clean
		X : table of feature vector each row of it is feature vector for a cell with id of indices it has size of cells in the T
		D : is the table of domain for each attribute
		Y : is the set of label for the cell in the T 
		W : table of weights that we learn in the learning section
		b : is the table that contains the biases that generated in learning part
		Y_pred : is the table of probabilities for don't know cells  
	
	"""
	
	table_name=['']*len(attributes)
	def __init__(self):
		self.attribute = {}
		for a in Dataset.attributes:
			self.attribute[a] = 0
		self.dataset_id=self.id_generator()
		self.table_name[0]=self.dataset_id
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
	
	def spec_tb_name(self,table_general_name):
		return self.table_name[self.attributes.index(table_general_name)]
		

