
class ColumnInfo:
	"""
	Data object that holds information about a column and the unique values it has.
	"""
	
	def __init__(self, name, dataType, uniqueValues=[]):
		"""
		:type name: string
		:param name: name of the column
		
		:type dataType: string
		:param dataType: either "continuous" or "discrete"
		
		:type uniqueValues: list
		:param uniqueValues: contains all unique data values for the given column
		"""
		self.name=name
		self.dataType=dataType
		self.uniqueValues=uniqueValues
		
