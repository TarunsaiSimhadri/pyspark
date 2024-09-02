from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("read/write").master("local[*]").getOrCreate()

df_csv = spark.read.csv("exported2_data.csv", header = True, inferSchema = True)

df_csv.write.csv("./exp.csv")

df_csv.show()

# df = df_csv.select(split)()