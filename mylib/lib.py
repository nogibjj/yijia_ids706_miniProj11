from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Function to create a Spark Session
def create_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

# Function to load data from a CSV file into a Spark DataFrame
def load_data(spark, file_path):
    return spark.read.option("header", "true").csv(file_path)

# Function to apply data transformation
def transform_data(df):
    # Calculate the average of "Temperature Minimum" and "Temperature Maximum" for each row
    df = df.withColumn(
        "Avg_Temperature",
        ((col("Temperature Minimum").cast("float") + col("Temperature Maximum").cast("float")) / 2)
    )
    return df

# Function to execute a simple Spark SQL query on existing columns
def query(df, spark):
    df.createOrReplaceTempView("weather_data")
    
    # Query to filter and group data
    sql_query = """
    SELECT Date, 
           AVG(CAST(`Temperature Minimum` AS FLOAT)) as avg_temp_min
    FROM weather_data
    WHERE CAST(`Temperature Maximum` AS FLOAT) > 75
    GROUP BY Date
    ORDER BY avg_temp_min DESC
    """
    result = spark.sql(sql_query)
    return result
