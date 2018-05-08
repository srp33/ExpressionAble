
class ColumnInfo:
	'Data object that holds information about a column and the unique values it has'
	
	def __init__(self, name, dataType, uniqueValues=[]):
		self.name=name
		self.dataType=dataType
		self.uniqueValues=uniqueValues
		
