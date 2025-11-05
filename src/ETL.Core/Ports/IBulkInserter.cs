using ETL.Core.Models;

namespace ETL.Core.Ports
{
    public interface IBulkInserter
    {
        Task BulkInsertAsync(IAsyncEnumerable<TripRecord> rows, CancellationToken ct = default);
        Task TransformDataAsync(CancellationToken ct = default);
        Task TruncateStagingAsync(CancellationToken ct = default);
        Task RemoveDuplicatesAsync(CancellationToken ct = default);
    }
}
