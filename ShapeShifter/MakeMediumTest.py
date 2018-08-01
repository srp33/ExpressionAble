from random import randint

f=open("TallData.tsv","w")
headers= "Sample\t"
for i in range(1,6):
	headers+="int"+str(i)+"\t"
for i in range(1,6):	
	headers+="discrete"+str(i)
	if i!=5:
		headers+="\t"
headers+="\n"
f.write(headers)
discretes=["hot", "warm", "lukewarm", "cold"]

for i in range(1,1000001):
	line="s"+str(i)+"\t"
	for j in range(1,6):
		line+=str(randint(0,100))+"\t"
	for j in range(1,6):
		line+=discretes[randint(0,3)]
		if j!=5:
			line +="\t"
	line+="\n"
	f.write(line)
