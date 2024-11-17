# yijia_ids706_miniProj10

## Python Template
This project uses Python and PySpark to perform data processing and analysis on a large dataset of weather data. The main objectives are to incorporate a Spark SQL query and execute a data transformation.

## CI/CD Badge
[![CI](https://github.com/nogibjj/yijia_ids706_miniProj10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/yijia_ids706_miniProj10/actions/workflows/cicd.yml)

## File Structure
- **`.devcontainer/`**: Contains the development container configuration (`devcontainer.json` and a Dockerfile) to ensure a consistent development environment.
- **`Makefile`**: Provides commands for setup, testing, linting, and formatting the project.
- **`.github/workflows/`**: Contains CI/CD workflows for GitHub, which trigger actions like setup, linting, and testing when code is pushed to the repository.
- **`rdu-weather-history.csv`**: Weather data for the Durham region.
- **`output_summary.md`**: A generated summary log, documenting the output of SQL queries and transformations performed during the data processing steps.

## Setup

### 1. Clone the Repository
```bash
git clone git@github.com:nogibjj/yijia_ids706_miniProj10.git
```

### 2. Open the Repository in GitHub Codespace
- Open the project in a GitHub Codespace and configure it to use the .devcontainer setup.
- Rebuild the container if necessary, ensuring Docker is running on your computer.

### 3. Install dependencies
Run the following command to install all required dependencies:

```bash
make install
```

## Usage
- make install: Installs dependencies specified in requirements.txt.
- make format: Formats Python files using Black.
- make lint: Lints Python files using Pylint, ignoring specific patterns.
- make test: Runs tests using pytest and generates a coverage report.

## Data Transformation and PySpark SQL Query Summary

- Data Transformation: Create an Avg_Temperature column by averaging the Temperature Minimum and Temperature Maximum values for each row, simplifying temperature analysis.

- PySpark SQL Query: Filters days with Temperature Maximum over 75, groups data by date, and calculates the average of Temperature Minimum for each date, ordering the results by highest average minimum temperature. This setup focuses analysis on warmer days, providing key insights into temperature trends.

## CI/CD Setup
- Location: .github/workflows/
- Description: Contains GitHub Actions workflows for CI/CD, which automatically run setup, linting, testing, and generate and push the log file when code is pushed to the repository.

