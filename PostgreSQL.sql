-- select the number of trips in january 15
SELECT COUNT(*)
	FROM 
nytaxi_trips
WHERE CAST(tpep_pickup_datetime AS DATE) = '2021-01-15'; 

-- largest tip for each day
SELECT
	CAST(tpep_pickup_datetime AS DATE),MAX(tip_amount)
FROM nytaxi_trips
	GROUP BY CAST(tpep_pickup_datetime AS DATE)
ORDER BY 2 DESC;

-- most popular destination
SELECT lt."Zone", lt2."Zone" AS popular_dest_central_park
FROM 
lookup_data_table AS lt
WHERE 
	lt."LocationID" = (SELECT 
	drop_off AS dropoff_id
FROM (
SELECT nt."DOLocationID" AS drop_off,COUNT(*) AS count_zones
	FROM nytaxi_trips AS nt
LEFT JOIN lookup_data_table AS lt
	ON nt."PULocationID" = lt."LocationID"
WHERE CAST(nt.tpep_pickup_datetime AS DATE) = '2021-01-14'
	AND lt."Zone" = 'Central Park'
GROUP BY nt."DOLocationID"
ORDER BY 2 DESC
LIMIT 1) AS subquery);

-- most expensive locations
SELECT CONCAT(pickup_zones, '/',dropoff_zones)
FROM
(SELECT CASE WHEN final_final_sub."Zone" IS NULL THEN 'Unknown'
ELSE final_final_sub."Zone" END AS pickup_zones,
CASE WHEN lt2."Zone" IS NULL THEN 'Unknown'
	ELSE lt2."Zone" END AS dropoff_zones
FROM
(SELECT *
	FROM (
(SELECT 
	CAST (LEFT(sl.pick_drop, strpos(sl.pick_drop,',') -1) AS INT) AS pickup_id,
	CAST(RIGHT(sl.pick_drop, LENGTH(sl.pick_drop) - strpos(sl.pick_drop, ',')) AS INT) AS dropoff_id
FROM (
SELECT 
	CONCAT(nt."PULocationID",',',nt."DOLocationID") 
	AS pick_drop, AVG(nt."total_amount") AS avg_amount
FROM nytaxi_trips nt
GROUP BY CONCAT(nt."PULocationID",',',nt."DOLocationID")
ORDER BY 2 DESC
LIMIT 1) AS sl)) AS fsl
LEFT JOIN lookup_data_table AS lt 
ON fsl.pickup_id = lt."LocationID"
WHERE fsl.pickup_id = lt."LocationID" OR fsl.dropoff_id = lt."LocationID") AS final_final_sub
LEFT JOIN lookup_data_table as lt2
ON final_final_sub.dropoff_id = lt2."LocationID") AS result_sub; 