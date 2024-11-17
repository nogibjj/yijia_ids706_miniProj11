# **yijia_ids706_miniProj11**

## **Overview**
This project demonstrates a **Databricks pipeline** for managing and analyzing weather data using **Spark**. It consists of three sequential steps:
1. **Extract**: Reads raw data from a publicly hosted CSV file.
2. **Transform**: Calculates average temperatures for each row and saves the results in Delta format.
3. **Query**: Filters and groups the transformed data based on specified conditions.

The pipeline workflow is automated using **Databricks Jobs**, with **CI/CD processes** implemented via GitHub Actions.

---

## **CI/CD Badge**


---

## **Databricks Setup**
Follow these steps to set up the pipeline in Databricks:

### **1. Setting Up a Cluster**
1. Navigate to the **Compute** section in Databricks.
2. Click **Create Cluster** and configure it with the default settings.
3. Start the cluster.

### **2. Connecting to GitHub**
1. Navigate to the **Workspace** tab.
2. Create a new Git folder and link it to the repository:
`https://github.com/nogibjj/yijia_ids706_miniProj11.git`


### **3. Installing Required Libraries**
No additional libraries need to be installed because:
- `pyspark` is pre-installed in Databricks clusters.
- Other libraries like `databricks-sql-connector` and `python-dotenv` are **not required** for this pipeline as it processes Delta tables and does not load `.env` files.

### **4. Creating Jobs**
1. Go to the **Jobs** tab in Databricks and create the following jobs:
- **Extract**: Runs the `extract.py` script.
- **Transform**: Runs the `transform_load.py` script.
- **Query**: Runs the `query.py` script.
2. Set task dependencies:
- **Transform** depends on **Extract**.
- **Query** depends on **Transform**.

---

## **Data Source and Sink**

### **Data Source**
The pipeline uses a publicly accessible CSV file as its data source:
- URL: [`rdu-weather-history.csv`](https://raw.githubusercontent.com/nogibjj/yijia_ids706_miniProj3/refs/heads/main/rdu-weather-history.csv)

### **Data Sink**
The outputs of each pipeline stage are saved in Delta format for scalability and efficiency:
1. **Extracted Data**: Saved at `/dbfs/tmp/extracted_data`.
2. **Transformed Data**: Saved at `/dbfs/tmp/transformed_data`.

---

## **Pipeline Workflow**
This pipeline implements an **ETL (Extract, Transform, Load)** process to analyze weather data using Python and PySpark:
1. **Extract**: Retrieves data from the source URL and saves it in Delta format at `/dbfs/tmp/extracted_data`.
2. **Transform**: Calculates the average temperature for each row and saves the transformed data in Delta format at `/dbfs/tmp/transformed_data`.
3. **Query**: Executes SQL queries on the transformed data to filter and group results.

Each step is executed in sequence, with downstream tasks depending on the successful completion of upstream tasks to ensure data consistency.

---

## **CI/CD Setup**

### **Workflow Configuration**
The pipeline is automated using GitHub Actions to ensure reliable and repeatable deployment. The CI/CD workflow performs the following tasks:
1. Installs dependencies to ensure compatibility.
2. Lints and formats the code to maintain consistency.
3. Runs tests to validate functionality.
4. Sets up environment variables for secure connection to Databricks.

### **Environment Setup in GitHub**
To set up secrets for secure access to Databricks:
1. Go to your GitHub repository.
2. Navigate to Settings > Secrets and variables > Actions.
3. Add the following secrets: SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN.
