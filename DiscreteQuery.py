class DiscreteQuery:
	'Data object that holds information about a query to be made on a parquet file'

	def __init__(self, columnName:str, values:list):
		self.columnName = columnName
		self.values = values
