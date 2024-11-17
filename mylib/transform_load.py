from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def transform_data(df):
    # Calculate the average of "Temperature Minimum" and "Temperature Maximum" for each row
    df = df.withColumn(
        "Avg_Temperature",
        ((col("Temperature Minimum").cast("float") + col("Temperature Maximum").cast("float")) / 2)
    )
    return df

def transform():
    spark = create_spark("Transform Data")
    # Read the extracted data
    df = spark.read.format("delta").load("/dbfs/tmp/extracted_data")
    transformed_df = transform_data(df)
    transformed_df.write.format("delta").save("/dbfs/tmp/transformed_data")
    print("Transform step completed: Data saved.")
