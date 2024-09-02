from pyspark import SparkContext

sc=SparkContext(appName="practice")
numbers = sc.parallelize([1, 2, 3, 4, 5, 5])
lines = sc.parallelize(["hello world", "hi world"])
result = lines.map(lambda x : x.split(" ")).collect()
result = lines.flatMap(lambda x : x.split(" ")).collect()
squared = numbers.filter(lambda x: x>1)
rdd = sc.textFile("path to file")

# result = squared.reduce(lambda x, y: x + y)
# result = numbers.distinct()
print(result)
sc.stop()