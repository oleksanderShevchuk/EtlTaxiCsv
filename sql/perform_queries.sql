-- Highest average tip per pickup location
SELECT TOP 1 PULocationID, AVG(tip_amount) AS AvgTip
FROM dbo.Trips
GROUP BY PULocationID
ORDER BY AvgTip DESC;

-- Top 100 longest fares by trip distance
SELECT TOP 100 *
FROM dbo.Trips
ORDER BY trip_distance DESC;

-- Top 100 longest fares by travel time
SELECT TOP 100 *
FROM dbo.Trips
ORDER BY trip_duration_seconds DESC;

-- Search by PULocationId with filters
SELECT *
FROM dbo.Trips
WHERE PULocationID = 132
  AND fare_amount > 20
  AND tpep_pickup_datetime BETWEEN '2024-01-01' AND '2024-02-01';
