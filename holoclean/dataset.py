#!/user/bin/env python
class Dataset:

	attributes = ['T', 'C_clean', 'C_dk', 'X', 'D', 'Y',
					'W', 'b', 'Y_prod', 'config']
	
	def __init__(self):
		self.attribute = {}
		for a in Dataset.attributes:
			self.attribute[a] = 0
