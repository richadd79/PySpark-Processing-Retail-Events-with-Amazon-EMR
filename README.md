# PySpark Processing Retail Events with Amazon EMR

# Introduction
An upstart retail company is growing their conversions rapidly and want to move their data to a data lake. 
Their data resides in S3 directory in json format.
In this project, we will build an ETL pipeline for a data lake hosted on S3.

The goals of the project are:
1. Load data from S3 and process it into analytics tables
2. Load them back in to S3 in parquet format.


# Dataset
The dataset used for this project is sourced from Kaggle [E-Commerce Data](https://www.kaggle.com/datasets/carrie1/ecommerce-data)


# Tooling
Here is an overview of the tools used for the execution of the project

`AWS S3` used to store unprocessed data and also as the final output of a Data Lake.

`EMR` as the big data solution used in spark processing the uprocessed data on S3. 

`EC2` as the computing capacity backbone for Spark.

`PYSPARK` as the main interface for Apacke Spark in python, writing spark etl pipeline.




# Spark ETL
This is how the ETL pipeline works

1. Read data from S3

    + `Purchase Events:` s3://retail-events-bkt/purchase_events/
    
2. Process data using Spark

   Transforms the data by splitting and creating 2 main analytical tables: `INVOICES` and `ITEMS`

3. Load data from analytical tables to S3

   Writes tables to `partitioned parquet files` on S3 directories

