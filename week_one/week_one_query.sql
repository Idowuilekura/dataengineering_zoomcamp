-- SELECT COUNT(*)
-- 	FROM 
-- green_taxi_data
-- 	WHERE CAST(DATE(lpep_pickup_datetime) AS VARCHAR) = '2019-01-15';

-- first query
with count_trip_each_day AS (
	SELECT CAST(DATE(lpep_pickup_datetime) AS VARCHAR) pickup_date, COUNT(*) trips_per_day
FROM
	green_taxi_data
GROUP BY 1
ORDER BY 2 DESC)

SELECT cted.pickup_date, cted.trips_per_day
FROM 
	count_trip_each_day cted 
LIMIT 1;

-- second query

WITH trip_distance_time AS (SELECT CAST(DATE(lpep_pickup_datetime) AS VARCHAR) 
							pickup_date,(lpep_dropoff_datetime - lpep_pickup_datetime) trip_time
FROM green_taxi_data),
	max_distance_time_day AS (SELECT pickup_date pickup_date,SUM(trip_time) biggest_trip_day
							 FROM trip_distance_time
							 GROUP BY pickup_date
							 ORDER BY 2 DESC)
					
SELECT pickup_date, biggest_trip_day
FROM max_distance_time_day
ORDER BY biggest_trip_day DESC
LIMIT 1;


-- third query
WITH two_passenger_counts AS (
	SELECT passenger_count, COUNT(*) total_passenger
FROM green_taxi_data
WHERE passenger_count = 2 AND CAST(DATE(lpep_pickup_datetime) AS VARCHAR) = '2019-01-01'
GROUP BY 1),
	three_passenger_counts AS (
	SELECT passenger_count, COUNT(*) total_passenger
	FROM green_taxi_data
	WHERE passenger_count = 3 AND CAST(DATE(lpep_pickup_datetime) AS VARCHAR) = '2019-01-01'
	GROUP BY 1)
	
SELECT passenger_count, total_passenger
FROM two_passenger_counts
UNION ALL
SELECT passenger_count, total_passenger
FROM three_passenger_counts;

-- last query
WITH customers_picked_astoria AS (
SELECT gtd."PULocationID" pickup_location_id,gtd.tip_amount tip_amount,
	nzt."Zone" pickup_zone,gtd."DOLocationID" dropoff_location_id
	FROM green_taxi_data gtd
INNER JOIN ny_zones_table nzt
ON gtd."PULocationID" = nzt."LocationID"
WHERE nzt."Zone" = 'Astoria'),
customers_dropped_astoria AS (
SELECT gtd."PULocationID" pickup_location_id,gtd.tip_amount tip_amount,
	nzt."Zone" dropoff_zone,gtd."DOLocationID" dropoff_location_id
	FROM green_taxi_data gtd
INNER JOIN ny_zones_table nzt
ON gtd."DOLocationID" = nzt."LocationID"),

dropoff_zone_astoria_tip AS (
SELECT cda.dropoff_zone dropoff_zone,cpa.pickup_zone ,cda.tip_amount tip_amount
FROM customers_dropped_astoria cda
INNER JOIN customers_picked_astoria cpa
ON cpa.pickup_location_id = cda.pickup_location_id
AND cpa.dropoff_location_id = cda.dropoff_location_id
WHERE cda.dropoff_zone != 'Astoria'),


	second_last AS (SELECT dzat.dropoff_zone dropoff_zone, MAX(dzat.tip_amount) total_tip
	FROM dropoff_zone_astoria_tip dzat
	GROUP BY 1
	ORDER BY 2 DESC)
SELECT * FROM second_last
ORDER BY total_tip DESC
LIMIT 1;