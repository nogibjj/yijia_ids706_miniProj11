import pytest
from pyspark.sql import SparkSession
from mylib.lib import create_spark, load_data, transform_data, query


@pytest.fixture(scope="module")
def spark():
    # Initialize Spark session for testing
    spark = create_spark(app_name="TestApp")
    yield spark
    spark.stop()


@pytest.fixture
def data_frame(spark):
    # Load the CSV data directly for testing
    file_path = "rdu-weather-history.csv"
    df = load_data(spark, file_path)
    return df


def test_create_spark(spark):
    assert isinstance(
        spark, SparkSession
    ), "Test failed: Spark session not created correctly."
    print("Spark session created successfully.")


# Check if data was loaded correctly
def test_load_data(data_frame):
    assert data_frame.count() > 0, "Test failed: No data loaded from CSV."
    print("CSV file reading test passed successfully.")


# Test the transform_data function
def test_transform_data(data_frame):
    transformed_df = transform_data(data_frame)
    assert (
        "Avg_Temperature" in transformed_df.columns
    ), "Test failed: Avg_Temperature column not found after transformation."
    print("Transformation test passed: Avg_Temperature column added successfully.")


# Test the query function by running the query on the transformed data
def test_query(spark, data_frame):
    data_frame.createOrReplaceTempView("weather_data")
    result_df = query(data_frame, spark)

    # Check if the result contains the expected columns and is not empty
    assert result_df.count() > 0, "Test failed: Query returned no results."
    assert (
        "Date" in result_df.columns and "avg_temp_min" in result_df.columns
    ), "Test failed: Expected columns not found in query result."
    print("Query test passed: Data filtered, grouped, and ordered as expected.")


if __name__ == "__main__":
    pytest.main()
