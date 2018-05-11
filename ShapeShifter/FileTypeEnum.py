from enum import Enum

class FileTypeEnum(Enum):
	"""
	This class represents an indicator for various file types
	"""
	CSV = 0
	JSON = 1
	Excel = 4
	HDF5 =5
	Feather = 6
	Parquet = 7
	MsgPack = 8
	Stata = 9
	Pickle = 10
	HTML = 11

