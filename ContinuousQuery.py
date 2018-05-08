from OperatorEnum import OperatorEnum
class ContinuousQuery:
	'Data object that encapsulates a continuous query, holding info on the column, operator, and value'
	def __init__(self, columnName:str, operator:OperatorEnum, value:float):
		self.columnName = columnName
		self.operator = operator
		self.value = value



