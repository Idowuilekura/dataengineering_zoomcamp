StructType(
    StructField('hvfhs_license_num', StringType(), True), 
    StructField('dispatching_base_num', StringType(), True), 
    StructField('pickup_datetime', TimestampType(), True), 
    StructField('dropoff_datetime', TimestampType(), True), 
    StructField('PULocationID', IntegerType(), True), 
    tructField('DOLocationID', IntegerType(), True), 
    StructField('SR_Flag', StringType(), True))


-- green format
types.StructType([
            types.StructField('VendorID', types.LongType(), True), 
            types.StructField('lpep_pickup_datetime', types.StringType(), True), 
            types.StructField('lpep_dropoff_datetime', types.StringType(), True), 
            types.StructField('store_and_fwd_flag', types.StringType(), True), 
            types.StructField('RatecodeID', types.LongType(), True), 
            types.StructField('PULocationID', types.LongType(), True), 
            types.StructField('DOLocationID', types.LongType(), True), 
            types.StructField('passenger_count', types.LongType(), True), 
            types.StructField('trip_distance', types.DoubleType(), True), 
            types.StructField('fare_amount', types.DoubleType(), True), 
            types.StructField('extra', types.DoubleType(), True), 
            types.StructField('mta_tax', types.DoubleType(), True), 
            types.StructField('tip_amount', types.DoubleType(), True), 
            types.StructField('tolls_amount', types.DoubleType(), True), 
            types.StructField('ehail_fee', types.DoubleType(), True), 
            types.StructField('improvement_surcharge', types.DoubleType(), True), 
            types.StructField('total_amount', types.DoubleType(), True), 
            types.StructField('payment_type', types.LongType(), True), 
            types.StructField('trip_type', types.LongType(), True), 
            types.StructField('congestion_surcharge', types.DoubleType(), True)])

types.StructType([
    types.StructField('VendorID', types.LongType(), True),
    types.StructField('tpep_pickup_datetime', types.StringType(), True), 
    types.StructField('tpep_dropoff_datetime', types.StringType(), True), 
    types.StructField('passenger_count', types.LongType(), True), 
    types.StructField('trip_distance', types.DoubleType(), True), 
    types.StructField('RatecodeID', types.LongType(), True), 
    types.StructField('store_and_fwd_flag', types.StringType(), True), 
    types.StructField('PULocationID', types.LongType(), True), 
    types.StructField('DOLocationID', types.LongType(), True), 
    types.StructField('payment_type', types.LongType(), True), 
    types.StructField('fare_amount', types.DoubleType(), True), 
    types.StructField('extra', types.DoubleType(), True), 
    types.StructField('mta_tax', types.DoubleType(), True), 
    types.StructField('tip_amount', types.DoubleType(), True), 
    types.StructField('tolls_amount', DoubleType(), True), 
    types.StructField('improvement_surcharge', types.DoubleType(), True), 
    types.StructField('total_amount', DoubleType(), True), types.StructField('congestion_surcharge', types.DoubleType(), True)])

-- rdd result
types.StructType([
    types.StructField('hour', types.TimestampType(), True), 
    types.StructField('zone', types.IntegerType(), True), 
    types.StructField('revenue', types.DoubleType(), True), 
    types.StructField('count', types.IntegerType(), True)])



