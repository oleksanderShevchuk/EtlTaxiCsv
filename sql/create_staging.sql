USE TaxiTrips;
GO

CREATE TABLE dbo.StagingTrips
(
    tpep_pickup_datetime VARCHAR(100),
    tpep_dropoff_datetime VARCHAR(100),
    passenger_count VARCHAR(10),
    trip_distance VARCHAR(50),
    store_and_fwd_flag VARCHAR(10),
    PULocationID VARCHAR(20),
    DOLocationID VARCHAR(20),
    fare_amount VARCHAR(50),
    tip_amount VARCHAR(50),
    RawLine NVARCHAR(MAX)
);
GO
