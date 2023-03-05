#!/usr/bin/env python
# coding: utf-8

import pyspark 
from pyspark.sql import SparkSession
import findspark
findspark.init()
from pyspark.sql import functions as F
import argparse 

parser = argparse.ArgumentParser(description='submit spark job')

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()
input_green = args.input_green
input_yellow = args.input_yellow

output = args.output
spark_session = SparkSession.builder.appName('spark_sql').getOrCreate()



df_green = spark_session.read.parquet(input_green)




# df_green.show()




df_yellow = spark_session.read.parquet(input_yellow)



# get the columns and combine them
df_green.columns



df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime','pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')


df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime','pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')

yellow_column = df_yellow.columns



green_column = df_green.columns





common_columns = []

for col in green_column:
    if col in yellow_column:
        common_columns.append(col)
    else:
        pass


df_green_sel = df_green.select(common_columns).withColumn('service_type',F.lit('green'))

df_yellow_sel = df_yellow.select(common_columns).withColumn('service_type',F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.registerTempTable('trips_data')

df_trips_data.createOrReplaceTempView('trips_data')


spark_session.sql("""
SELECT service_type,
count(1)
FROM 
trips_data 
GROUP BY 1;
""").show()



df_result = spark_session.sql("""
 SELECT 
    -- Reveneue grouping 
    PULocationID as revenue_zone,
    date_trunc('month', pickup_datetime) as revenue_month, 

    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,
    sum(congestion_surcharge) as revenue_monthly_congestion_surcharge,

    -- Additional calculations
    avg(passenger_count) as avg_montly_passenger_count,
    avg(trip_distance) as avg_montly_trip_distance

    FROM trips_data
    GROUP BY 
        1,2,3;
""")


df_result.show()


df_result.coalesce(1).write.mode('overwrite').parquet(output)
