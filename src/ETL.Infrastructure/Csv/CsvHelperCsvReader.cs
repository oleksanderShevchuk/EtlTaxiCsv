using System.Globalization;
using System.Runtime.CompilerServices;
using CsvHelper.Configuration;
using CsvHelper;
using ETL.Core.Ports;

namespace ETL.Infrastructure.Csv
{
    public class CsvHelperCsvReader : ICsvReader
    {
        private StreamReader? _reader;
        private CsvReader? _csv;

        public async ValueTask DisposeAsync()
        {
            _csv?.Dispose();
            _reader?.Dispose();
            await Task.CompletedTask;
        }

        public async IAsyncEnumerable<IDictionary<string, string>> ReadRowsAsync(string path, [EnumeratorCancellation] CancellationToken ct = default)
        {
            _reader = new StreamReader(path);
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                TrimOptions = TrimOptions.Trim,
                BadDataFound = null,
                IgnoreBlankLines = true,
                MissingFieldFound = null
            };
            _csv = new CsvReader(_reader, config);
            await _csv.ReadAsync();
            _csv.ReadHeader();
            if (_csv.HeaderRecord == null)
                await _csv.ReadAsync();

            var headers = _csv.HeaderRecord ?? Array.Empty<string>();

            while (await _csv.ReadAsync())
            {
                ct.ThrowIfCancellationRequested();
                var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var h in headers)
                {
                    var val = _csv.GetField(h);
                    dict[h] = val?.Trim() ?? string.Empty;
                }
                yield return dict;
            }
        }
    }
}
