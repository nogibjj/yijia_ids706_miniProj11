from pyspark.sql import SparkSession

def create_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def query(df, spark):
    df.createOrReplaceTempView("weather_data")
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

def query_data():
    spark = create_spark("Query Data")
    # Read the transformed data
    df = spark.read.format("delta").load("/dbfs/tmp/transformed_data")
    result = query(df, spark)
    result.show()
    print("Query step completed: Results displayed.")
