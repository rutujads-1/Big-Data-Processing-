import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
#TASK 1
from datetime import datetime
from pyspark.sql.functions import from_unixtime

#TASK 2, 3, 5, 6
from pyspark.sql.functions import count, month, desc, rank, col,dense_rank, concat, lit, avg, dayofmonth
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
from pyspark.sql.functions import sum, month

#TASK3  specific
from pyspark.sql.window import Window

#TASK 8
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
from pyspark.sql.functions import col
from graphframes import GraphFrame

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Rideshare")\
        .getOrCreate()

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=9:
                return False
            float(fields[6])
            float(fields[7])
            return True
        except:
            return False
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    #TASK 1- Q1
    # Load rideshare_data table from shared bucket
    rideshare_data_path = f"s3a://{bucket}/rideshare_data.csv"
    rideshare_data = spark.read.format("csv").option("header", "true").load(rideshare_data_path)
    rideshare_data.show(5)

    # Load taxi_zone_lookup table from shared bucket
    taxi_zone_lookup_path = f"s3a://{bucket}/taxi_zone_lookup.csv"
    taxi_zone_lookup = spark.read.format("csv").option("header", "true").load(taxi_zone_lookup_path)
    taxi_zone_lookup.show(5)
    
    #TASK 1- Q2
    # Intermediate join between rideshare_data and taxi_zone_lookup on pickup_location
    intermediate_join = rideshare_data.join(taxi_zone_lookup, rideshare_data.pickup_location == taxi_zone_lookup.LocationID, "left")
    # Renaming columns
    intermediate_join = intermediate_join.select(
        [rideshare_data[col] for col in rideshare_data.columns] +
        [taxi_zone_lookup["Borough"].alias("Pickup_Borough"),
         taxi_zone_lookup["Zone"].alias("Pickup_Zone"),
         taxi_zone_lookup["service_zone"].alias("Pickup_service_zone")]
    )

    # Final join using intermediate_join and taxi_zone_lookup on dropoff_location
    final_join = intermediate_join.join(taxi_zone_lookup, intermediate_join.dropoff_location == taxi_zone_lookup.LocationID, "left")
    # Renaming columns
    final_join = final_join.select(
        [intermediate_join[col] for col in intermediate_join.columns] +
        [taxi_zone_lookup["Borough"].alias("Dropoff_Borough"),
         taxi_zone_lookup["Zone"].alias("Dropoff_Zone"),
         taxi_zone_lookup["service_zone"].alias("Dropoff_service_zone")]
    )
    #printing result after join
    final_join.printSchema()
    final_join.show(truncate=False)
    
    #TASK 1- Q3
    final_join= final_join.withColumn("date", from_unixtime("date").cast("date"))
    final_join.printSchema()
    #TASK 1-Q4
    final_join.printSchema()
    y=final_join.count()
    
    print(f"\n")
    print(f"\n")
    print(f"\n")
    print(f'The number of rows of the dataframe after joining is {y}')
    print(f"\n")
    print(f"\n")
    print(f"\n")
    
    # TASK 2- Q1
    res1= final_join.withColumn("month", month("date"))

    # Grouping by business and month, and count the number of trips
    trip_counts= res1.groupBy("business", "month").agg(count("*").alias("trip_count"))
    
    # Showing the result
    trip_counts.show()
      
    # Converting trip_counts DataFrame to Pandas DataFrame
    trip_counts_pd = trip_counts.toPandas()
    
    # Getting current date and time for timestamp
    date_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    
    # CSV file name with timestamp
    csv_filename = f'trip_counts_{date_time}.csv'
    
    # Convert Pandas DataFrame to CSV format in memory
    csv_buffer = StringIO()
    trip_counts_pd.to_csv(csv_buffer, index=False)
    
    # Upload CSV file to S3 bucket
    s3 = boto3.client('s3',
                      endpoint_url=f'http://{s3_endpoint_url}',
                      aws_access_key_id=s3_access_key_id,
                      aws_secret_access_key=s3_secret_access_key)
    s3.put_object(Bucket=s3_bucket, Key=csv_filename, Body=csv_buffer.getvalue())
    
    # Download the CSV file from S3
    s3.download_file(s3_bucket, csv_filename, csv_filename)
    
    print(f'CSV file "{csv_filename}" saved in S3 bucket "{s3_bucket}" and downloaded locally.')
    
    
    # TASK 2- Q2

    #type casting to float
    res2= final_join.withColumn("rideshare_profit", final_join["rideshare_profit"].cast("float"))
    
    # Extracting month from date column
    res2= final_join.withColumn("month", month("date"))
    
    # Grouping by business and month, and sum the rideshare_profit
    profits_by_bm = res2.groupBy("business", "month").agg(sum("rideshare_profit").alias("total_profit"))
    
    # Showing the result
    profits_by_bm.show()
    #downloading the data in csv
    # Convert profits_by_business_month DataFrame to Pandas DataFrame
    profits_by_bm_pd = profits_by_bm.toPandas()
    
    # Get current date and time for timestamp
    date_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    
    # CSV file name with timestamp
    csv_filename = f'profits_by_bm_{date_time}.csv'
    
    # Convert Pandas DataFrame to CSV format in memory
    csv_buffer = StringIO()
    profits_by_bm_pd.to_csv(csv_buffer, index=False)
    
    # Upload CSV file to S3 bucket
    s3 = boto3.client('s3',
                      endpoint_url=f'http://{s3_endpoint_url}',
                      aws_access_key_id=s3_access_key_id,
                      aws_secret_access_key=s3_secret_access_key)
    s3.put_object(Bucket=s3_bucket, Key=csv_filename, Body=csv_buffer.getvalue())
    
    # Download the CSV file from S3
    s3.download_file(s3_bucket, csv_filename, csv_filename)
    
    print(f'CSV file "{csv_filename}" saved in S3 bucket "{s3_bucket}" and downloaded locally.')
    
    #TASK 2- Q3
    res3=final_join.withColumn("driver_total_pay", final_join["driver_total_pay"].cast("float"))

    # Extracting the month from date column
    res3= final_join.withColumn("month", month("date"))

    # Grouping by business and month, and sum the driver_total_pay
    earnings_by_bm = res3.groupBy("business", "month").agg(sum("driver_total_pay").alias("total_earnings"))
    #earnings_by_bm.show()

    #downloading the data in csv
    # Convert earnings_by_business_month DataFrame to Pandas DataFrame
    earnings_by_bm_pd = earnings_by_bm.toPandas()
    
    # Get current date and time for timestamp
    date_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    
    # CSV file name with timestamp
    csv_filename = f'earnings_by_bm_{date_time}.csv'
    
    # Convert Pandas DataFrame to CSV format in memory
    csv_buffer = StringIO()
    earnings_by_bm_pd.to_csv(csv_buffer, index=False)
    
    # Upload CSV file to S3 bucket
    s3 = boto3.client('s3',
                      endpoint_url=f'http://{s3_endpoint_url}',
                      aws_access_key_id=s3_access_key_id,
                      aws_secret_access_key=s3_secret_access_key)
    s3.put_object(Bucket=s3_bucket, Key=csv_filename, Body=csv_buffer.getvalue())
    
    # Download the CSV file from S3
    s3.download_file(s3_bucket, csv_filename, csv_filename)
    
    print(f'CSV file "{csv_filename}" saved in S3 bucket "{s3_bucket}" and downloaded locally.')

    # TASK 3- Q1
    # Extracting the month from timestamp column
    res4 = final_join.withColumn("Month", month("date"))

    # Grouping by columns Pickup_Borough and Month, and count the number of trips
    pickup_counts = res4.groupBy("Pickup_Borough", "Month").agg(count("*").alias("trip_count"))

    # Ranking the pickup boroughs within each month based on the trip count
    window_spec = Window.partitionBy("Month").orderBy(desc("trip_count"))
    pickup_counts = pickup_counts.withColumn("rank", dense_rank().over(window_spec))
    # Sorting the output
    pickup_counts = pickup_counts.orderBy("Month", desc("trip_count"))
    # Filter fot top 5
    top_pickup_b = pickup_counts.filter(col("rank") <= 5).select("Pickup_Borough", "Month", "trip_count")
    
    #Showing the result
    top_pickup_b.show(25)    

    #TASK 3- Q2
    r7 = final_join.withColumn("Month", month("date"))

    # Grouping by Dropoff_Borough and Month, and count the number of trips
    dropoff_cnts = r7.groupBy("Dropoff_Borough", "Month").agg(count("*").alias("trip_count"))

    # Ranking the dropoff boroughs within each month based on trip count
    window_spec = Window.partitionBy("Month").orderBy(desc("trip_count"))
    dropoff_cnts = dropoff_cnts.withColumn("rank", dense_rank().over(window_spec))

    # Sorting the output
    dropoff_cnts = dropoff_cnts.orderBy("Month", desc("trip_count"))

    # Filter the top 5
    top_dropoffboroughs = dropoff_cnts.filter(col("rank") <= 5).select("Dropoff_Borough", "Month", "trip_count")
    # Showing the result
   top_dropoffboroughs.show(25)

    # TASK 3- Q3
    # type casting to float
    r8 = final_join.withColumn("driver_total_pay", final_join["driver_total_pay"].cast("float"))

    # Grouping by Pickup Borough and Dropoff Borough, and sum the 'driver_total_pay' field to calculate total profit for each route
    route_profit = r8.groupBy("Pickup_Borough", "Dropoff_Borough").agg(sum("driver_total_pay").alias("total_profit"))

    # Concatenate Pickup Borough and Dropoff Borough columns to create the route
    route_profit = route_profit.withColumn("Route", concat(col("Pickup_Borough"), lit(" to "), col("Dropoff_Borough")))

    # Selecting required columns
    route_profit = route_profit.select("Route", "total_profit")
    # Sorting the routes
    sorted_routes_p = route_profit.orderBy(desc("total_profit"))

    # Showing the result
    sorted_routes_p.show(30)

     #TASK 4 -Q1
    average_pay_by_time_of_day = final_join.groupBy("time_of_day").agg(avg("driver_total_pay").alias("average_drive_total_pay"))

    # Sorting the output 
    sorted_average_pay = average_pay_by_time_of_day.orderBy("average_drive_total_pay", ascending=False)

    # Showing the result
    sorted_average_pay.show()

     #TASK 4- Q2
    # Grouping by 'time_of_day' and calculate the average 'trip_length'
    average_trip_length_by_time_of_day = final_join.groupBy("time_of_day").agg(avg("trip_length").alias("average_trip_length"))

    # Sorting the output 
    sort_average_trip_length = average_trip_length_by_time_of_day.orderBy("average_trip_length", ascending=False)

    # Showing the result
    sort_average_trip_length.show()

    #TASK 4- Q3
    joined_data = sorted_average_pay.join(sort_average_trip_length, "time_of_day")

    # Calculate the average earning per mile
    joined_data = joined_data.withColumn("average_earning_per_mile", col("average_drive_total_pay") / col("average_trip_length"))

    # Selecting the required columns
    res = joined_data.select("time_of_day", "average_earning_per_mile")
    #sorting the result
    sorted_res = res.orderBy("average_earning_per_mile", ascending=False)
    # Show the result
    sorted_res.show()

     #TASK 5- Q1
    # Filtering the data for January
    jan_data = final_join.filter(month(final_join["date"]) == 1)

    # Calculate the average waiting time per day
    # Grouping by day of the month
    grouped_data = jan_data.groupBy(dayofmonth("date").alias("day"))

    # Calculate the average waiting time per day
    average_waiting_time_per_day = grouped_data.agg(avg("request_to_pickup").alias("average_waiting_time"))

    # Sorting the result by day
    sorted_average_waiting_time = average_waiting_time_per_day.orderBy("day")
    sorted_average_waiting_time.show()
    
    # Convert sorted_avg_waiting_time_pd DataFrame to Pandas DataFrame
    sorted_average_waiting_time_pd = sorted_average_waiting_time.toPandas()
    
    # Get current date and time for timestamp
    date_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    
    # CSV file name with timestamp
    csv_filename = f'sorted_avg_waiting_time_{date_time}.csv'
    
    # Convert Pandas DataFrame to CSV format in memory
    csv_buffer = StringIO()
    sorted_average_waiting_time_pd.to_csv(csv_buffer, index=False)
    
    # Upload CSV file to S3 bucket
    s3 = boto3.client('s3',
                      endpoint_url=f'http://{s3_endpoint_url}',
                      aws_access_key_id=s3_access_key_id,
                      aws_secret_access_key=s3_secret_access_key)
    s3.put_object(Bucket=s3_bucket, Key=csv_filename, Body=csv_buffer.getvalue())
    
    # Download the CSV file from S3
    s3.download_file(s3_bucket, csv_filename, csv_filename)
    
    print(f'CSV file "{csv_filename}" saved in S3 bucket "{s3_bucket}" and downloaded locally.')
    

    # TASK 5- Q2
   # Filtering the DataFrame 
    days_exceed_300 = average_waiting_time_per_day.filter(col("average_waiting_time") > 300)

    # Showing the result
    days_exceed_300.show()

    # TASK 6 -Q1

    # Grouping by 'Pickup_Borough' and 'time_of_day' and count the trips
    tripcounts = final_join.groupBy("Pickup_Borough", "time_of_day").agg(count("*").alias("trip_count"))
    
    # Filtering the DataFrame for trip counts greater than 0 and less than 1000
    filter_data = tripcounts.filter((col("trip_count") > 0) & (col("trip_count") < 1000))
    
    # Showing the result
    filter_data.show()

    # TASK 6- Q2
    from pyspark.sql.functions import col
    # Filtering the DataFrame for evening time records
    evening_trips = final_join.filter(col("time_of_day") == "evening")
    
    # Grouping by 'Pickup_Borough' and 'time_of_day' and count the trips
    evening_trip_cnts = evening_trips.groupBy("Pickup_Borough", "time_of_day").agg(count("*").alias("trip_count"))
    
    # Showing the result
    evening_trip_cnts.show()

    # TASK 6- Q3
    from pyspark.sql.functions import col

    # Filtering the DataFrame for trips starting in Brooklyn and ending in Staten Island
    brooklyn_to_statenisland_trips = final_join.filter((col("Pickup_Borough") == "Brooklyn") & (col("Dropoff_Borough") == "Staten Island"))

    res10= brooklyn_to_statenisland_trips.select("Pickup_Borough", "Dropoff_Borough", "Pickup_Zone")
    
    # Counting the number of trips
    num_of_trips = brooklyn_to_statenisland_trips.count()
    
    # Showing 10 samples
    res10.show(10, truncate=False)
    
    Show the total number of trips
    print(f'\n')
    print("Number of trips from Brooklyn to Staten Island:", num_of_trips)
    print(f'\n')

     #TASK 7

    # Concatenating Pickup_Zone and Dropoff_Zone to create the Route column
    final_join = final_join.withColumn("Route", concat(col("Pickup_Zone"), lit(" to "), col("Dropoff_Zone")))
    
    # Grouping by Route and count the trips for Uber and Lyft separately
    route_counts = final_join.groupBy("Route", "business").agg(count("*").alias("count"))
    
    # Pivot the data to have separate columns for Uber and Lyft counts
    pivoted_counts = route_counts.groupBy("Route").pivot("business").agg(sum("count").alias("count")).fillna(0)
    
    # Calculate the total count by summing the counts for Uber and Lyft
    pivoted_counts = pivoted_counts.withColumn("total_count", col("Uber") + col("Lyft"))
    
    # Selecting the top 10 routes based on total count
    top_routes = pivoted_counts.orderBy(desc("total_count")).limit(10)
    
    # Showing the result table including route, uber_count, lyft_count, and total_count
    top_routes.show()
    from pyspark.sql.functions import concat, col, lit, sum, desc

    # Concatenating Pickup_Zone and Dropoff_Zone to create the Route column
    final_join = final_join.withColumn("Route", concat(col("Pickup_Zone"), lit(" to "), col("Dropoff_Zone")))
    
    # Grouping by Route and count the trips for Uber and Lyft separately
    route_counts = final_join.groupBy("Route", "business").agg(count("*").alias("count"))
    
    # Pivot the data to have separate columns for Uber and Lyft counts
    pivoted_counts = route_counts.groupBy("Route").pivot("business").agg(sum("count").alias("count")).fillna(0)
    
    # Calculate the total count by summing the counts for Uber and Lyft
    pivoted_counts = pivoted_counts.withColumn("total_count", col("Uber") + col("Lyft"))
    
    # Selecting the top 10 routes 
    top_routes = pivoted_counts.orderBy(desc("total_count")).limit(10)
    
    #Showing the result table including route, uber_count, lyft_count, and total_count with desired column names
    top_routes.select(col("Route").alias("Route"),
                      col("Uber").alias("uber_count"),
                      col("Lyft").alias("lyft_count"),
                      col("total_count")).show()

    spark.stop()
