using ETL.Core.Models;

namespace ETL.Core.Ports
{
    public interface IDuplicateDetector
    {
        bool IsDuplicate(in TripKey key);
    }
}
