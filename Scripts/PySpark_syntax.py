from pyspark import SparkContext

sc=SparkContext("local", "simple app")

#Read in text file
textFile = sc.textFile('/hadoop_work/books/4300-0.txt')

#First five lines
print(textFile.take(5))

#Count number of lines
print(textFile.count())

#Count lines with 'Ulysses'
linesWithUlysses = textFile.filter(lambda line: "Ulysses" in line)
print(linesWithUlysses.count())
print(linesWithUlysses.take(2))

#Find line with most words
def max(a,b):
	if a>b:
		return a
	else:
		return b

print(textFile.map(lambda line: len(line.split())).reduce(max))
print(textFile.map(lambda line: len(line.split()))).reduce(lambda a,b: a if (a>b) else b)



#Wordcounts with MapReduce

#Creates a tuple for every word in every line (word, 1)
wordCounts = textFile.flatMap(lambda line: line.split()).map(lambda word: (word,1))

#Aggregate by key, for each unique key(word), give count
wordCounts2 = wordCounts.reduceByKey(lambda a,b: a+b)
print(wordCounts2.take(5))

#All in one syntax
wordCounts_f=textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a,b: a+b)
print(wordCounts_f.take(5))

#Show most frequent words
freq_Words = wordCounts_f.sortBy(keyfunc=lambda a: a[1], ascending=False)
print(freq_Words.take(5))

#Alternate syntax
freq_Words2 = wordCounts_f.sortBy(lambda pair: pair[1], ascending=False)
print(freq_Words.take(5))
