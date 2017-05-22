
//to test run spark-shell -i textfile_read.scala
//to run properly against cluster will need to compile code and package into JAR 
//Execute the JAR using Spark-Submit to distribute across the cluster
//Within spark-shell just do :load textfile_read.scala
//Read in text file
val textFile = sc.textFile("/hadoop_work/books/4300-0.txt")

//First five lines
textFile.take(5)

//Count number of lines
textFile.count()

//Count lines with 'Ulysses'
val linesWithUlysses = textFile.filter(line=>line.contains("Ulysses"))
linesWithUlysses.count()
linesWithUlysses.take(2)

//Find line with most words
textFile.map(line => line.split(" ").size).reduce((a,b)=> if (a>b) a else b)

//Count word frequency
val wordCounts = textFile.flatMap(line=>line.split(" ")).map(word => (word,1)).reduceByKey((a,b)=>a+b)
wordCounts.take(10)

val freqWords = wordCounts.sortBy(_._2, ascending=false) //rdd.sortBy(pair => pair._2)
freqWords.take(10)