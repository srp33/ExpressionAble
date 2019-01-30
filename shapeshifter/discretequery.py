class DiscreteQuery:
	"""
	Data object that holds information about a discrete query to be made on a dataset.
	"""

	def __init__(self, columnName:str, values:list):
		"""
		:type columnName: string
		:param columnName: the name of a column which will be examined

		:type values: list of strings
		:param values: list of values to which the examined column will be compared
		"""

		self.columnName = columnName
		self.values = values
