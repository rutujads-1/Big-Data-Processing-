import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

from functools import reduce
from pyspark.sql.functions import col, lit, when, desc
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import graphframes
from graphframes import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .appName("graphframes") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    #loading the data
    # Load rideshare_data table from shared bucket
    rideshare_data_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/rideshare_data.csv"
    rideshare_data = spark.read.format("csv").option("header", "true").load(rideshare_data_path)
    rideshare_data.show(5)

    # Load taxi_zone_lookup table from shared bucket
    taxi_zone_lookup_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/taxi_zone_lookup.csv"
    taxi_zone_lookup = spark.read.format("csv").option("header", "true").load(taxi_zone_lookup_path)
    taxi_zone_lookup.show(5)
    
    # Define vertex schema for taxi zones
    v_Schema = StructType([
        StructField("id", IntegerType(), False),  # ID field for vertex
        StructField("Borough", StringType(), True),  # Borough field from taxi zone lookup
        StructField("Zone", StringType(), True),  # Zone field from taxi zone lookup
        StructField("service_zone", StringType(), True)  # Service zone field from taxi zone lookup
    ])
    
    # Define edge schema for rideshare data
    e_Schema = StructType([
        StructField("src", IntegerType(), False),  # Source location ID
        StructField("dst", IntegerType(), False),  # Destination location ID
    ])

    print("Vertex Schema:")
    print(f'\n')
    for field in v_Schema.fields:
         print(field.name, field.dataType)
        
          
    print("\nEdge Schema:")
    for field in e_Schema.fields:
        print(field.name, field.dataType)

    # TASK 8 - Q2
    # Create vertices DataFrame

    #type casting to integer
    taxi_zone_lookup = taxi_zone_lookup.withColumn("id", col("LocationID").cast("int"))
    
    # Selecting required columns for vertices DataFrame
    ver_df = taxi_zone_lookup.select("id", "Borough", "Zone", "service_zone")
    
    # Selecting required columns for edges DataFrame
    edge_df = rideshare_data.select(col("pickup_location").alias("src"), col("dropoff_location").alias("dst"))
    
    # Showing 10 samples of the vertices DataFrame
    print("Vertices DataFrame:")
    ver_df.show(10, truncate=False)
    
    # # Showing 10 samples of the edges DataFrame
    print("Edges DataFrame:")
    edge_df.show(10, truncate=False)

     # TASK 8 -Q3
    # Defining vertices DataFrame
    # Convert 'src' and 'dst' columns in edges dataframe to integer
    edge_df = edge_df.withColumn("src", col("src").cast("int"))
    edge_df = edge_df.withColumn("dst", col("dst").cast("int"))
    
    # Create GraphFrame
    graph = GraphFrame(ver_df, edge_df)
    
    # Create graph dataframe with columns 'src', 'edge', and 'dst'
    graph_dframe = graph.find("(a)-[e]->(b)").select(col("a").alias("src"), col("e").alias("edge"), col("b").alias("dst"))
    
    # Printing 10 samples of the graph dataframe
    graph_dframe.show(10, truncate=False)

    #TASK 8 -Q4
    
    # Defining the condition for filtering connected vertices with the same Borough and service_zone
    condt= "a.Borough = b.Borough AND a.service_zone = b.service_zone"
    
    # Finding connected vertices with the same Borough and service_zone
    connected_vertices_in_same_zone = graph.find("(a)-[]->(b)").filter(condt)
    
    # Select columns 'id' from source and destination vertices, along with 'Borough' and 'service_zone'
    selected_res = connected_vertices_in_same_zone.select(col("a.id").alias("id"), col("b.id").alias("id"), "a.Borough", "a.service_zone")
    
    # Showing 10 samples from the result
    selected_res.show(10, truncate=False)

    #TASK 8 -Q5

    #Performing PageRank on the graph dataframe
    pagerank_res = graph.pageRank(resetProbability=0.17, tol=0.01)
    
    #Getting the vertices with their PageRank values
    pagerank_ver = pagerank_res.vertices
    
    #Sorting vertices by descending order of PageRank values
    sort_pagerank_ver = pagerank_ver.orderBy(desc("pagerank"))
    
    #Showing the top 5 samples of the results
    sort_pagerank_ver.show(5)
    
    #Select only the required columns ('id' and 'pagerank')
    selected_pagerank_ver = sort_pagerank_ver.select("id", "pagerank")
    
    #Show the selected columns
    selected_pagerank_ver.show(5) 
    spark.stop()
