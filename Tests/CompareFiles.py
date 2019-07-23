import sys
#Tests output files against each other. If files are identical, prints "Identical". Otherwise, identifies the first location (line number and position) where they are not identical

filePath1 = sys.argv[1]
filePath2 = sys.argv[2]

try:
	f1=open(filePath1)
	f2=open(filePath2)

	f1_line = f1.readline()
	f2_line= f2.readline()

	line=1
	match=True
	while f1_line !='' or f2_line !='':

		f1_line = f1_line.rstrip()
		f2_line = f2_line.rstrip()

		if f1_line != f2_line:
			for i in range(len(f1_line)):
				if f1_line[i] != f2_line[i]:
					print(filePath1 + " and " + filePath2 + ": FAIL. Difference at line: "+str(line) + " position: " + str(i))
					match=False
					break
			break

		f1_line = f1.readline()
		f2_line= f2.readline()
		line+=1
	if match:
		print(filePath1 + " and " + filePath2 + ": PASS")
except Exception as e:
	print(filePath1 + " and " +filePath2 + ": FAIL")
	print("Error: " +str(e))
