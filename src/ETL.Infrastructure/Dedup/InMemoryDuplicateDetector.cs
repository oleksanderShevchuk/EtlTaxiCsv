using System.Collections.Concurrent;
using ETL.Core.Models;
using ETL.Core.Ports;

namespace ETL.Infrastructure.Dedup
{
    public class InMemoryDuplicateDetector : IDuplicateDetector
    {
        private readonly ConcurrentDictionary<string, byte> _seen = new();

        public bool IsDuplicate(in TripKey key)
        {
            var k = key.ToString();
            return !_seen.TryAdd(k, 0); // true if already existed -> duplicate
        }
    }
}
