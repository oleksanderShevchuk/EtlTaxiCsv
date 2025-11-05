USE TaxiTrips;
GO

CREATE TABLE dbo.StagingTrips
(
    tpep_pickup_datetime DATETIME2(3) NULL,
    tpep_dropoff_datetime DATETIME2(3) NULL,
    passenger_count SMALLINT NULL,
    trip_distance DECIMAL(10,3) NULL,
    store_and_fwd_flag VARCHAR(3) NULL,
    PULocationID INT NULL,
    DOLocationID INT NULL,
    fare_amount DECIMAL(10,2) NULL,
    tip_amount DECIMAL(10,2) NULL,
    RawLine NVARCHAR(MAX)
);
GO
