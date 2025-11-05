using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using ETL.Core.Models;
using ETL.Core.Ports;
using ETL.Core.Utils;
using Serilog;

namespace ETL.Core.Services
{
    public class ImportService
    {
        private readonly ICsvReader _csvReader;
        private readonly IDuplicateDetector _duplicateDetector;
        private readonly IFileWriter _duplicateWriter;
        private readonly IBulkInserter _bulkInserter;
        private readonly IClock _clock;
        private readonly ILogger _logger;

        public ImportService(
            ICsvReader csvReader,
            IDuplicateDetector duplicateDetector,
            IFileWriter duplicateWriter,
            IBulkInserter bulkInserter,
            IClock clock,
            ILogger logger)
        {
            _csvReader = csvReader;
            _duplicateDetector = duplicateDetector;
            _duplicateWriter = duplicateWriter;
            _bulkInserter = bulkInserter;
            _clock = clock;
            _logger = logger;
        }

        public async Task RunAsync(string csvPath, string duplicatesPath, string sourceTimeZoneId, CancellationToken ct = default)
        {
            var accepted = Channel.CreateUnbounded<TripRecord>();
            var writer = accepted.Writer;

            _logger.Information("Starting CSV import from {CsvPath}", csvPath);

            var producer = Task.Run(async () =>
            {
                try
                {
                    await foreach (var row in _csvReader.ReadRowsAsync(csvPath, ct))
                    {
                        ct.ThrowIfCancellationRequested();
                        try
                        {
                            if (!DateTime.TryParse(row["tpep_pickup_datetime"], CultureInfo.InvariantCulture, DateTimeStyles.None, out var pickupLocal))
                            {
                                _logger.Warning("Invalid pickup datetime, skipping row");
                                continue;
                            }
                            if (!DateTime.TryParse(row["tpep_dropoff_datetime"], CultureInfo.InvariantCulture, DateTimeStyles.None, out var dropLocal))
                            {
                                _logger.Warning("Invalid dropoff datetime, skipping row");
                                continue;
                            }

                            var pickupUtc = _clock.ConvertToUtc(pickupLocal, sourceTimeZoneId);
                            var dropUtc = _clock.ConvertToUtc(dropLocal, sourceTimeZoneId);

                            if (!short.TryParse(row.GetValueOrDefault("passenger_count", "0"), out var passenger)) passenger = 0;
                            decimal.TryParse(row.GetValueOrDefault("trip_distance", "0"), out var tripDistance);
                            var saf = row.GetValueOrDefault("store_and_fwd_flag", "").Trim();
                            saf = saf.Equals("Y", StringComparison.OrdinalIgnoreCase) ? "Yes"
                                 : saf.Equals("N", StringComparison.OrdinalIgnoreCase) ? "No" : saf;

                            int.TryParse(row.GetValueOrDefault("PULocationID", "0"), out var pu);
                            int.TryParse(row.GetValueOrDefault("DOLocationID", "0"), out var doid);
                            decimal.TryParse(row.GetValueOrDefault("fare_amount", "0"), out var fare);
                            decimal.TryParse(row.GetValueOrDefault("tip_amount", "0"), out var tip);

                            var key = new TripKey(pickupUtc, dropUtc, passenger);
                            if (_duplicateDetector.IsDuplicate(key))
                            {
                                var raw = row.ContainsKey("RawLine") ? row["RawLine"] : string.Join(',', row.Values);
                                await _duplicateWriter.AppendLineAsync(duplicatesPath, raw, ct);
                                continue;
                            }

                            var rec = new TripRecord(pickupUtc, dropUtc, passenger, tripDistance, saf, pu, doid, fare, tip);
                            await writer.WriteAsync(rec, ct);
                        }
                        catch (Exception ex)
                        {
                            _logger.Warning("Error processing row: {0}", ex.Message);
                        }
                    }
                }
                finally
                {
                    writer.Complete();
                }
            }, ct);

            var consumer = Task.Run(async () =>
            {
                var reader = accepted.Reader;
                async IAsyncEnumerable<TripRecord> GetAsync([EnumeratorCancellation] CancellationToken token = default)
                {
                    while (await reader.WaitToReadAsync(token))
                    {
                        while (reader.TryRead(out var item))
                        {
                            yield return item;
                        }
                    }
                }

                _logger.Information("Inserting into staging table...");
                await _bulkInserter.BulkInsertAsync(GetAsync(ct), ct);

                _logger.Information("Transforming data into main table...");
                await _bulkInserter.TransformDataAsync(ct);

                _logger.Information("Cleaning staging table...");
                await _bulkInserter.TruncateStagingAsync(ct);

                _logger.Information("Remove duplicates...");
                await _bulkInserter.RemoveDuplicatesAsync(ct);
            }, ct);

            await Task.WhenAll(producer, consumer);
            _logger.Information("Import finished successfully.");
        }
    }
}