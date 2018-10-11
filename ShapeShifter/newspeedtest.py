import time

from .SSFile import SSFile

supported_file_types = {"Small": ["Parquet", "CSV", "Excel", "HDF5", "JSON", "MsgPack", "Pickle", "SQLite", "Stata", "TSV"],
                          "Wide": ["Parquet", "CSV", "HDF5", "MsgPack", "Pickle", "Stata", "TSV"],
                          "Tall": ["Parquet", "CSV", "Excel", "HDF5", "JSON", "MsgPack", "Pickle", "SQLite", "Stata", "TSV"]}

input_file_paths = {"Small": "Tests/Speed/Small/SmallTest.", "Wide": "Tests/Speed/Wide/WideTest.", "Tall": "Tests/Speed/Tall/TallTest."}

output_file_paths = {"Small": "Tests/Speed/Output/SmallOutput.", "Wide": "Tests/Speed/Output/WideOutput.", "Tall": "Tests/Speed/Output/TallOutput."}

extensions = {"Parquet": "pq", "CSV": "csv", "Excel": "xlsx", "JSON": "json", "MsgPack": "mp", "Pickle": "pkl",
        "HDF5": "hdf", "SQLite": "db", "Stata": "dta", "TSV": "tsv"}

filters = {"read_whole_file": None, "1_categorical": "discrete1=='hot'", "1_numeric": "int1>20",
           "1_of_each": "discrete1=='hot' and int1>20", "2_categorical":"discrete1=='hot' and discrete2=='cold'",
           "2_numeric": "int1>20 and int2<70", "2_of_each": "discrete1=='hot' and discrete2=='cold' and int1>20 and int2<70"}

columns_needed = {"read_whole_file": [], "1_categorical": ["Sample", "discrete1"], "1_numeric": ["Sample", "int1"],
           "1_of_each": ["Sample", "discrete1", "int1"], "2_categorical": ["Sample", "discrete1", "discrete2"],
           "2_numeric": ["Sample", "int1", "int2"], "2_of_each": ["Sample", "int1", "int2", "discrete1", "discrete2"]}

header = 'Operation\tFormat\tSize\tFilter\tSeconds\n'
output_lines=[]

for file_size in supported_file_types:
    for file_type in supported_file_types[file_size]:
        file_path = input_file_paths[file_size] + extensions[file_type]
        file_obj = SSFile.factory(file_path)
        for filter_description in filters:
            t1 = time.time()
            df = file_obj.read_input_to_pandas(columnList=columns_needed[filter_description])
            t2 = time.time()
            seconds = str(t2-t1)
            line = "Read\t" + file_type + "\t" + file_size + "\t" + filter_description + "\t" + seconds + "\n"
            output_lines.append(line)

for file_size in supported_file_types:
    for file_type in supported_file_types[file_size]:
        in_file = SSFile.factory(input_file_paths[file_size] + "pq")
        for filter_description in filters:
            out_file = SSFile.factory(output_file_paths[file_size] + extensions[file_type])
            df = in_file._filter_data(columnList=columns_needed[filter_description])
            t1 = time.time()
            out_file.write_to_file(df)
            t2 = time.time()
            seconds = str(t2-t1)
            line = "Write\t" + file_type + "\t" + file_size + "\t" + filter_description + "\t" + seconds + "\n"
            output_lines.append(line)

with open('NewSpeedResults.tsv', 'w') as f:
    f.write(header)
    for line in output_lines:
        f.write(line)
