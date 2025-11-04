namespace ETL.Core.Ports
{
    public interface ICsvReader : IAsyncDisposable
    {
        // Read rows as raw dictionary (transform in core)
        IAsyncEnumerable<IDictionary<string, string>> ReadRowsAsync(string path, CancellationToken ct = default);
    }
}
