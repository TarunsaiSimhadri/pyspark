from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("practice3").getOrCreate()

data = [("tarun", 21, "masters"),
        ("varun", 25, "masters"),
        ("murali", 23, "degree"),
        ("tarun", 24, "B.Tech")]

schema = ["Name", "Age", "Qualification"]

df = spark.createDataFrame(data, schema)

df.createOrReplaceTempView("df")

result = spark.sql("SELECT * FROM df WHERE Name = 'tarun'")

result.show()