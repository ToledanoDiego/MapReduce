from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w']+")

#-- list of global values I use either to store some lists so it is easier to compute with them either some counters so things can happens only once
counter = 0
counter2 = 0
datas = []
result = []

class task(MRJob):

#-- mapper that get the id value, the all the lines whiout the ids, and initialize the output from the inputed file
	def mapper(self, _, line):
		
		global counter
		global datas
		data=line.split()
		id=data[0].strip()
		data.pop(0)
		datas.append(data)
		result.append([])
		if counter < 10:
			yield 1, 1
		counter = counter + 1
		
#-- reducer that get the setup and return the output, here is an example of how it works: if 6 is in row 8, then output[6].append(8)		
	def reducer1(self,key, list_of_values):
		global counter2
		global datas
		global result
		counter3 = 1
		if counter2 < 1:
			for id in datas:
				for data in id:		

					result[int(data)-1].append(counter3)
					
				counter3 = counter3 + 1
			for x in result:
				yield str(result.index(x)+1), [i for n, i in enumerate(x) if i not in x[:n]]
		counter2 = counter2 + 1

#-- basic step
	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer1)]

#-- basic start
if __name__ == '__main__':
	task.run()
