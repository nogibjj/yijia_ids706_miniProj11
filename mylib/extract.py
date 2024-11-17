from pyspark.sql import SparkSession

def create_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark):
    # URL to the CSV file
    url = "https://raw.githubusercontent.com/nogibjj/yijia_ids706_miniProj3/refs/heads/main/rdu-weather-history.csv"
    print(f"Reading data from: {url}")
    # Read CSV directly from URL
    df = spark.read.option("header", "true").csv(url)
    return df

def extract():
    spark = create_spark("Extract Data from URL")
    df = load_data(spark)
    df.write.format("delta").save("/dbfs/tmp/extracted_data")
