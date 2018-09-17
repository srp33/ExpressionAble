from random import randint
from random import shuffle
#todo make sure all the col numbers are in a randomized set
f=open("MediumTest.tsv","w")
headers= "Sample\t"
colNumbers = list(range(1,101))
shuffle(colNumbers)

for i in colNumbers:
	headers+="int"+str(i)+"\t"
for i in range(0,100):
	headers+="discrete"+str(colNumbers[i])
	if i!=99:
		headers+="\t"
headers+="\n"
f.write(headers)
discretes=["hot", "warm", "lukewarm", "cold"]

for i in range(1,1001):
	line="s"+str(i)+"\t"
	for j in range(1,101):
		line+=str(randint(0,100))+"\t"
	for j in range(1,101):
		line+=discretes[randint(0,3)]
		if j!=100:
			line +="\t"
	line+="\n"
	f.write(line)
