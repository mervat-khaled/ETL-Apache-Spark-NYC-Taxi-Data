# ETL-Apache-Spark-NYC-Taxi-Data

# Overview
The goal of this project is to do some ETL (Extract, Transform, and Load) In NYC Taxi Data and its geographical information Using Apache Spark, performing various transformations using Spark's python API "PySpark" and SQL language. And finally saving the processed data into CSVs file partitioned by the number of executors on spark session.

# Transformation

The Spark ETL process involves the following transformation steps on the  data:

#### Date Format Change: Convert date columns to a specific date format as required.
#### Column Split: Split specified columns into multiple columns as required.
#### Column Concatenation: Combine multiple columns to create new composite columns.
#### Column Dropping: Remove unnecessary columns that have more than %90 null values.
#### DataFrame Join: Combine data from multiple data frames to create a unified dataset.

For Further details see these Notebooks: (NYC_taxi_ETL1 .ipynb)
