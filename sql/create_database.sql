CREATE DATABASE TaxiTrips;
GO
USE TaxiTrips;
GO

CREATE TABLE dbo.Trips
(
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    tpep_pickup_datetime DATETIME2(3) NOT NULL,
    tpep_dropoff_datetime DATETIME2(3) NOT NULL,
    passenger_count SMALLINT NOT NULL,
    trip_distance DECIMAL(10,3) NOT NULL,
    store_and_fwd_flag VARCHAR(3) NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    fare_amount DECIMAL(10,2) NOT NULL,
    tip_amount DECIMAL(10,2) NOT NULL,
    trip_duration_seconds AS DATEDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) PERSISTED
);
GO

CREATE NONCLUSTERED INDEX IX_Trips_PULocation_Tip ON dbo.Trips (PULocationID) INCLUDE (tip_amount);
CREATE NONCLUSTERED INDEX IX_Trips_TripDistance ON dbo.Trips (trip_distance DESC);
CREATE NONCLUSTERED INDEX IX_Trips_TripDuration ON dbo.Trips (trip_duration_seconds DESC);
CREATE NONCLUSTERED INDEX IX_Trips_PU_search ON dbo.Trips (PULocationID, tpep_pickup_datetime);
GO
