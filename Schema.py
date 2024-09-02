from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

spark = SparkSession.builder.appName('practice').getOrCreate()

schema = StructType([
    StructField("Sno", IntegerType(), nullable=False),
    StructField("Date", DateType(), nullable=True),
    StructField("Time", StringType(), nullable=True),
    StructField("State/UnionTerritory", StringType(), nullable=True),
    StructField("ConfirmedIndianNational", IntegerType(), nullable=False),
    StructField("ConfirmedForeignNational", IntegerType(), nullable=True),
    StructField("Cured", StringType(), nullable=True),
    StructField("Deaths", StringType(), nullable=True),
    StructField("Confirmed", StringType(), nullable=True),
])

df = spark.read.schema(schema).csv("covid_19_india.csv", header = True)

df.printSchema()

df.show()

