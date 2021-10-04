from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w']+")

#-- list of global values I use either to store some lists so it is easier to compute with them either some counters so things can happens only once
values = []
keys = []
counter = 0

class task(MRJob):

#-- basic mapper that get the age value from the inputed file
	def mapper(self, _, line):
		data=line.split(", ")
		if len(data)>=2:
			age=data[0].strip()
			yield (age, 1)
		
#-- basic reducer that count ages 
	def reducer1(self, key, list_of_values):
		global values
		global keys
		sumA = sum(list_of_values)
		yield key, sumA
		keys.append(key)
		values.append(sumA)

#-- reducer that output the maximum number of age and remove it from the map
	def reducer2(self, key, list_of_values):
		global counter
		global values
		global keys		
		if counter < 1:
			while len(values):
				yield max(values), keys[values.index(max(values))]
				keys.pop(values.index(max(values)))
				values.remove(max(values))
		counter = counter + 1

#-- basic step
	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer1),MRStep(reducer=self.reducer2)]

#-- basic start
if __name__ == '__main__':
	task.run()
