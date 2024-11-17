from pyspark.sql import SparkSession

def create_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, file_path):
    return spark.read.option("header", "true").csv(file_path)

def extract():
    # File path on DBFS
    file_path = "/dbfs/FileStore/tables/rdu-weather-history.csv"
    spark = create_spark("Extract Data")
    df = load_data(spark, file_path)
    df.write.format("delta").save("/dbfs/tmp/extracted_data")
    print("Extract step completed: Data saved.")
