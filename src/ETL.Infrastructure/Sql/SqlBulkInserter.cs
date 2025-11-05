using ETL.Core.Models;
using ETL.Core.Ports;
using Microsoft.Data.SqlClient;

namespace ETL.Infrastructure.Sql
{
    public class SqlBulkInserter : IBulkInserter
    {
        private readonly string _connectionString;
        private readonly string _destinationTable;
        private readonly int _batchSize;
        public SqlBulkInserter(string connectionString, string destinationTable, int batchSize = 50000)
        {
            _connectionString = connectionString;
            _destinationTable = destinationTable;
            _batchSize = batchSize;
        }

        public async Task BulkInsertAsync(IAsyncEnumerable<TripRecord> rows, CancellationToken ct = default)
        {
            using var reader = new TripRecordDataReader(rows);
            using var conn = await GetOpenConnectionAsync(ct);
            using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null)
            {
                DestinationTableName = _destinationTable,
                BatchSize = _batchSize,
                BulkCopyTimeout = 0
            };

            bulk.ColumnMappings.Add("tpep_pickup_datetime", "tpep_pickup_datetime");
            bulk.ColumnMappings.Add("tpep_dropoff_datetime", "tpep_dropoff_datetime");
            bulk.ColumnMappings.Add("passenger_count", "passenger_count");
            bulk.ColumnMappings.Add("trip_distance", "trip_distance");
            bulk.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
            bulk.ColumnMappings.Add("PULocationID", "PULocationID");
            bulk.ColumnMappings.Add("DOLocationID", "DOLocationID");
            bulk.ColumnMappings.Add("fare_amount", "fare_amount");
            bulk.ColumnMappings.Add("tip_amount", "tip_amount");

            await Task.Run(() => bulk.WriteToServer(reader), ct);
        }

        public async Task TransformDataAsync(CancellationToken ct)
        {
            var query = $@"
                INSERT INTO dbo.Trips (
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    store_and_fwd_flag,
                    PULocationID,
                    DOLocationID,
                    fare_amount,
                    tip_amount
                )
                SELECT 
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    store_and_fwd_flag,
                    PULocationID,
                    DOLocationID,
                    fare_amount,
                    tip_amount
                FROM dbo.StagingTrips
                WHERE trip_distance > 0 
                  AND passenger_count BETWEEN 1 AND 6
                  AND fare_amount > 0;";

            using var conn = await GetOpenConnectionAsync(ct);
            using var cmd = new SqlCommand(query, conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task TruncateStagingAsync(CancellationToken ct)
        {
            var truncate = $"TRUNCATE TABLE dbo.StagingTrips;";
            using var conn = await GetOpenConnectionAsync(ct);
            using var cmd = new SqlCommand(truncate, conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task RemoveDuplicatesAsync(CancellationToken ct = default)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            var duplicatesQuery = @"
                WITH Duplicates AS (
                    SELECT 
                        tpep_pickup_datetime,
                        tpep_dropoff_datetime,
                        passenger_count,
                        COUNT(*) AS cnt
                    FROM dbo.Trips
                    GROUP BY 
                        tpep_pickup_datetime,
                        tpep_dropoff_datetime,
                        passenger_count
                    HAVING COUNT(*) > 1
                )
                SELECT t.*
                FROM dbo.Trips t
                JOIN Duplicates d
                    ON t.tpep_pickup_datetime = d.tpep_pickup_datetime
                    AND t.tpep_dropoff_datetime = d.tpep_dropoff_datetime
                    AND t.passenger_count = d.passenger_count;";

            using var cmd = new SqlCommand(duplicatesQuery, conn);
            using var reader = await cmd.ExecuteReaderAsync(ct);

            var duplicates = new List<string> { "tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,fare_amount,tip_amount" };

            while (await reader.ReadAsync(ct))
            {
                var line = string.Join(",",
                    reader["tpep_pickup_datetime"],
                    reader["tpep_dropoff_datetime"],
                    reader["passenger_count"],
                    reader["trip_distance"],
                    reader["fare_amount"],
                    reader["tip_amount"]);
                duplicates.Add(line);
            }

            await reader.CloseAsync();

            if (duplicates.Count > 1)
            {
                var duplicatesPath = Path.Combine(AppContext.BaseDirectory, "duplicates.csv");
                await File.WriteAllLinesAsync(duplicatesPath, duplicates, ct);
            }

            var deleteQuery = @"
                WITH CTE AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
                               ORDER BY Id
                           ) AS rn
                    FROM dbo.Trips
                )
                DELETE FROM CTE WHERE rn > 1;";

            using var deleteCmd = new SqlCommand(deleteQuery, conn);
            await deleteCmd.ExecuteNonQueryAsync(ct);
        }

        private async Task<SqlConnection> GetOpenConnectionAsync(CancellationToken ct)
        {
            var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync(ct);
            return conn;
        }
    }
}
