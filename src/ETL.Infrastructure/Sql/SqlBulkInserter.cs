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
        public SqlBulkInserter(string connectionString, string destinationTable = "dbo.Trips", int batchSize = 50000)
        {
            _connectionString = connectionString;
            _destinationTable = destinationTable;
            _batchSize = batchSize;
        }

        public async Task BulkInsertAsync(IAsyncEnumerable<TripRecord> rows, CancellationToken ct = default)
        {
            using var reader = new TripRecordDataReader(rows);
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync(ct);
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

            // SqlBulkCopy has a synchronous WriteToServer(IDataReader) method;
            // no async variant run on threadpool to avoid blocking async context
            await Task.Run(() => bulk.WriteToServer(reader), ct);
        }
    }
}
