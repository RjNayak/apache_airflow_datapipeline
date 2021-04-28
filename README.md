<h1 align="center">Apache Airflow Data pipeline</h1>

<!-- <p align="center">
  <a href="#dart-about">About</a> &#xa0; | &#xa0;
  <a href="#sparkles-features">Features</a> &#xa0; | &#xa0;
  <a href="#rocket-technologies">Technologies</a> &#xa0; | &#xa0;
  <a href="#white_check_mark-requirements">Requirements</a> &#xa0; | &#xa0;
  <a href="#checkered_flag-starting">Starting</a> &#xa0; | &#xa0;
  <a href="#memo-license">License</a> &#xa0; | &#xa0;
  <a href="https://github.com/{{YOUR_GITHUB_USERNAME}}" target="_blank">Author</a>
</p> -->

<br>

## About

The application is a simple data pipeline script for collecting data from csv files. The csv files are sourced from NYC govt (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) portal and contains data of NYC Taxi and Limousine Association. The data contains trip details of yellow taxi cabs for the month of January,2019. The other csv file contains look up values for Locations IDs in the first CSV.
<br>
Files used are listed below and can be downloaded from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
<br>

yellow_tripdata_2019-01.csv
<br>
zone_lookup.csv
<br>
The data pipeline collects the data from the 2 csv files and run some aggregations to find the ranking of most favourite drop-off location for a specific location. The ranking is done based on number of passengers travel from the pickup location to the drop-ff location cumulatively.
The data pipeline also stores the top five ranked records toa history table.

## Features

Data pipeline: The Data pipeline collects, transforms and stores the data to database

Flask service endpoint: The Flask service provides easy way to query top 5 favourite drop-off location for a specific pick-up location for a specific month

## Technologies

The following tools were used in this project:

- [Python](https://www.python.org/)
- [Airflow](https://airflow.apache.org/)
- [Flask](https://flask.palletsprojects.com/en/1.1.x/)
- [SQlAlchemy](https://www.sqlalchemy.org/)
- [psycopg2](https://pypi.org/project/psycopg2/)
- [PostgreSQL](https://www.postgresql.org/)

## Requirements

Before starting this data pipeline you need to have [Python](https://www.python.org/),[Postgres](https://www.postgresql.org/),[Airflow](https://airflow.apache.org/) and [Git](https://git-scm.com) installed.

## Running

```bash
# Clone this project
$ git clone https://github.com/RjNayak/apache_airflow_datapipeline

# Access
$ cd apache_airflow_datapipeline

# Install dependencies
$ pip install -r requirements.txt

# To run the pipeline

# copy below files to airflow dags folder
  db_config.py
  simple_bash.py

# copy the csv files to desired location and  updated the path in simple_bash.py

# star Apache airflow server and scheduler
$ cd airflow
$ airflow webserver

# open another terminal
$ airflow scheduler

# The airflow webserver will initialize in the <http://localhost:8080>
# login name and password would be  - airflow

# find the DAG and enabled it and then start the DAG

# Once the data pipeline run successfully, open another terminal and start the Flask server
$ python query_service.py

```

<a href="#top">Back to top</a>
