from pyspark import SparkContext
logFile = "file:///usr/local/Cellar/apache-spark/2.4.5/README.md"
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: {}, lines with b: {}" .format(numAs, numBs))