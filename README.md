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

For Further details see these Notebooks:
[NYC_taxi_ETL1 .ipynb](https://github.com/mervat-khaled/ETL-Apache-Spark-NYC-Taxi-Data/blob/main/NYC_taxi_ETL1%20.ipynb)
[NYC_taxi_ETL2 .ipynb](https://github.com/mervat-khaled/ETL-Apache-Spark-NYC-Taxi-Data/blob/main/NYC_taxi_ETL2%20.ipynb)

# Technologies Used
Dockerized Apache spark 3.5.0 to process the data, and store the results into CSVs Files.

#### Spark image environment:

![Screenshots/image_config.png](Screenshots/image_config.png)

# Dataset:
We have a sample of NYC taxi data, which can be downloaded from [here](http://www.andresmh.com/nyctaxitrips/), this sample containes 100000 records/rows. Eaach row of the file header represents a single taxi ride in CSV format. For each ride, we have some attributes of the cab (a hashed version of the medallion number) as well as the driver (a hashed version of hack license, which is what licenses to drive taxies are called), some temporal information about when the trip started and ended, and the longitude/latitude coordinates for where the passengers(s) were picked up and dropped off.

we are mainly intersted in each Trip's:

* Some Unique ID for the car (license)
* Pick-up location
* Pick-up time 
* Drop-off location
* Drop-off time

# Problem:

 We need to compute one important statistic utilization. Utilization is the fraction of time that a cap is on the road and is occupied by one or more passengers. One factor that impacts utilization is the passenger's destination: a cab that drops off passengers near Union Square at midday is much more likely to find its next fare in just a minute or two, whereas a cab that drops someone off at 2 AM on Staten Island may have to drive all the way back to Manhatten before it find its next fare. 

We need to compute:

1. The average time it takes for a taxi to find its next fare(trip) per destination borough,
2. The number of trips that started and ended within the same borough.

# Steps:
To carry out the required analysis, we had to deal with two types of data: temporal data, such as dates and times, and geospatial information, like points of longitude and latitude and spatial boundaries.
As the data given in the taxi ride data set shows just the longitude and latitude of both the pickup and drop-off locations, we needed to enrich this data set with boroughs of the respective locations. For this, we had to load another data set that specifies the boundaries of each borough in GeoJson format, data is supplied as a separate [file](https://github.com/mervat-khaled/ETL-Apache-Spark-NYC-Taxi-Data/blob/main/data/nyc-boroughs.geojson?short_path=a7cec63). So we created a data frame out of it and broadcasted it to different workers to join it with the largest data frame. 
To enrich the taxi ride data set with pick up and drop off boroughs names, we needed to query the GeoJson data for the name of the borough for which the long/lat of the pick up drop off belongs. To achieve this goal, we used the geometry library [shapely](https://shapely.readthedocs.io/en/stable/) which provides several APIs to handle geometric shapes, among which to query about the inclusion of shape in another shape in another shape.
To let Spark handle these, we defined them as user-defined functions UDFs, as follows: 

