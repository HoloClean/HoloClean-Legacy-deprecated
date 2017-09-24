#!/user/bin/env python
class Dataset:

	attributes = ['id','T', 'C_clean', 'C_dk', 'X', 'D', 'Y',
					'W', 'b', 'Y_prod', 'config']
	
	def __init__(self):
		self.attribute = {}
		for a in Dataset.attributes:
			self.attribute[a] = 0
	def setatrribute(self,value,attr):
		self.attribute[attr]=value
		return self.attribute[attr]
	def getattribute(self,attr):
		return self.attribute[attr]
