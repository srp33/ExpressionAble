from enum import Enum

class FileTypeEnum(Enum):
	"""
	This class represents an indicator for various file types
	"""
	CSV = 0
	JSON = 1
	Excel =2 
	HDF5 =3
	Feather = 4
	Parquet = 5
	MsgPack = 6
	Stata = 7
	Pickle = 8
	HTML = 9
	TSV = 10
	ARFF = 11
	SQLite = 12
	GCT = 13

