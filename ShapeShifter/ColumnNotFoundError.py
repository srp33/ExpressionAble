import pandas as pd

class ColumnNotFoundError(Exception):
	def __init__(self, missingColumns):
		self.missingColumns = missingColumns
		
