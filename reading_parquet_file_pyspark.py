from pyspark.sql import SparkSession

path = ""
file_name = ""
parquet_file = path + file_name

spark_session = SparkSession.builder.appName("ParquestFileSession").getOrCreate()
par_dataF = spark_session.read.parquet(parquet_file)
par_dataF.show()
