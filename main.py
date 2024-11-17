from mylib.lib import create_spark, load_data, transform_data, query


def write_to_markdown(file_path, title, comment, dataframe):
    """Append DataFrame content as a Markdown table in a file with comments."""
    with open(file_path, "a") as file:
        # Write the section comment and title
        file.write(f"### {title}\n\n")
        file.write(f"{comment}\n\n")

        # Convert DataFrame rows to Markdown table format
        file.write("| " + " | ".join(dataframe.columns) + " |\n")
        file.write("|" + "|".join(["---"] * len(dataframe.columns)) + "|\n")

        for row in dataframe.collect():
            file.write("| " + " | ".join([str(item) for item in row]) + " |\n")

        file.write("\n")


def main():
    # Initialize Spark Session
    spark = create_spark("RDU Weather Analysis")

    output_file = "output_summary.md"
    file_path = "rdu-weather-history.csv"

    # Load the dataset
    df = load_data(spark, file_path)
    write_to_markdown(
        output_file,
        "Loaded Dataset",
        "Here is the result of loading the original data from 'rdu-weather-history.csv'.",
        df,
    )

    # Transform the data
    transformed_df = transform_data(df)
    write_to_markdown(
        output_file,
        "Transformed Data",
        "This is the transformed data where we calculated the average temperature by combining 'Temperature Minimum' and 'Temperature Maximum' for each row.",
        transformed_df,
    )

    # Run SQL query on transformed data
    sql_result_df = query(transformed_df, spark)
    write_to_markdown(
        output_file,
        "Query Result",
        "Based on the transformed data, this query calculates the average 'Temperature Minimum' for each date where 'Temperature Maximum' is above 75.",
        sql_result_df,
    )

    # Stop Spark Session
    spark.stop()
    print(f"Report saved as {output_file}")


if __name__ == "__main__":
    main()
