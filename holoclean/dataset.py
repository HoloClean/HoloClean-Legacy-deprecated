import random
from datetime import datetime

class Dataset:

	attributes = ['id','Init', 'C_clean', 'C_dk','dc_f_mysql','dc_f_dd', 'Feature_', 'Domain', 'Labels',
					'Weights', 'biases', 'Probabilities', 'config','dc_f1']
	
	"""
	
	Each element stand for some data that the holoclean needs or create:
	
		id : is the unique id for the dataset and it will be used in registering and retrieving data 
		Init : the initial data that get to the database from a file that user give
		C_clean : it is table with index of clean cells 
		C_dk : is table of indices that we don't know they are noisy or clean
		Feature : table of feature vector each row of it is feature vector for a cell with id of indices it has size of cells in the T
		Domain : is the table of domain for each attribute
		Labels : is the set of label for the cell in the T 
		Weights : table of weights that we learn in the learning section
		biases : is the table that contains the biases that generated in learning part
		Probabilities : is the table of probabilities for don't know cells  
		dc_f_mysql,dc_f_dd,config,dc_f1: are attributes only for testing. They will be removed
	
	"""
	
	table_name=['']*len(attributes)
	def __init__(self):
		"""TODO.

        	Parameters
        	----------
        	parameter : type
           	This is a parameter

        	Returns
        	-------
        	describe : type
            	Creates the table_names for each attribute for the dataset
        	"""
		self.attribute = {}
		for a in Dataset.attributes:
			self.attribute[a] = 0
		self.dataset_id=self._id_generator()
		self.table_name[0]=self.dataset_id
		for i in range(1,len(self.attributes)):
			self.table_name[i]=self.dataset_id+'_'+self.attributes[i]
			
	#Internal methods
	def _id_generator(self):
		"""This function create 
		a random id from the system time
		"""	
		r=random.seed(datetime.now())
		return str(random.random())[2:]

	#Getter
	def getattribute(self,attr):
		return self.table_name[attr]
	
	
	
	def spec_tb_name(self,table_general_name):
		"""TODO return the name of the table for this dataset"""
		return self.table_name[self.attributes.index(table_general_name)]
		

