from pyspark.sql import SparkSession 
#from pyspark import SparkContext

spark=SparkSession.builder.appName("simple").getOrCreate()

#Read in text file
textFile = spark.read.csv('/hadoop_work/data/HMEQ.csv', header=True)
print(textFile.show())

#Register View to use Spark sql
textFile.createOrReplaceTempView("text1")

simple_query = spark.sql('''SELECT BAD, LOAN 
	                        FROM text1 
	                        WHERE LOAN < 2100''')

print(simple_query.collect())
print(simple_query.count())