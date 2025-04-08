# Big Data Processing: Rideshare Data Analysis Using PySpark

## Description

This project involves large-scale data processing and analysis of NYC rideshare data using PySpark. The goal is to extract insights about trip patterns, profits, driver earnings, and route efficiencies using advanced Spark operations and graph-based techniques.

## Objectives

-Join rideshare data with NYC taxi zones

-Perform aggregations for business analysis

-Visualize trip volume, profits, and driver pay

-Identify top-performing routes, boroughs, and time segments

-Build a graph to explore network relationships and apply PageRank

## Technologies & Libraries Used

-Python

-PySpark

-Pandas

-AWS S3 (for data storage)

-GraphFrames (for graph processing)

-Jupyter Notebook

## Project Structure

```
Big-Data-Processing-/

├── notebooks/              # Jupyter notebooks and PySpark scripts

├── outputs/                   # CSV outputs and processed data

├── README.md               # Project summary and details

```
## Key Features

-Data joins using pickup/dropoff locations and taxi zone lookup

-Date parsing from UNIX timestamp to readable formats

-Monthly trip counts, platform profits, and driver earnings

-Most popular boroughs & routes

-Graph-based analysis to identify connected zones and influential nodes

-PageRank algorithm for understanding high-traffic areas

## Visualizations

-Monthly trip count & profit histograms

-Driver earnings by business and time-of-day

-Top 30 most profitable routes

-Waiting time trend in January

-PageRank score visualization for location nodes

## Insights

-Uber consistently outperforms Lyft in terms of trips and profits

-Driver earnings correlate strongly with trip volume

-Most rides occur in a few high-demand boroughs

-Evening and late-night periods yield higher per-mile earnings

-Top zones can be identified and optimized via graph-based processing

