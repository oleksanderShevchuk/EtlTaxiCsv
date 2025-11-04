using ETL.Core.Models;

namespace ETL.Core.Ports
{
    public interface IBulkInserter
    {
        Task BulkInsertAsync(IAsyncEnumerable<TripRecord> rows, CancellationToken ct = default);
    }
}
