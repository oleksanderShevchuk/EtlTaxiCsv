using System.Data;
using ETL.Core.Models;

namespace ETL.Infrastructure.Sql
{
    public class TripRecordDataReader : IDataReader
    {
        private readonly IEnumerator<TripRecord> _enumerator;
        private readonly Dictionary<string, int> _nameToIndex;
        private bool _hasRow;

        public TripRecordDataReader(IAsyncEnumerable<TripRecord> rows)
        {
            var list = new List<TripRecord>();
            var t = Task.Run(async () =>
            {
                await foreach (var r in rows)
                    list.Add(r);
            });
            t.GetAwaiter().GetResult();

            _enumerator = list.GetEnumerator();

            _nameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                ["tpep_pickup_datetime"] = 0,
                ["tpep_dropoff_datetime"] = 1,
                ["passenger_count"] = 2,
                ["trip_distance"] = 3,
                ["store_and_fwd_flag"] = 4,
                ["PULocationID"] = 5,
                ["DOLocationID"] = 6,
                ["fare_amount"] = 7,
                ["tip_amount"] = 8
            };
        }

        private TripRecord Current { get; set; } = default!;

        public int FieldCount => 9;

        public bool Read()
        {
            if (_enumerator.MoveNext())
            {
                Current = _enumerator.Current;
                return true;
            }
            return false;
        }

        public object GetValue(int i)
        {
            return i switch
            {
                0 => Current.PickupUtc,
                1 => Current.DropoffUtc,
                2 => Current.PassengerCount,
                3 => Current.TripDistance,
                4 => Current.StoreAndFwdFlag,
                5 => Current.PULocationId,
                6 => Current.DOLocationId,
                7 => Current.FareAmount,
                8 => Current.TipAmount,
                _ => throw new IndexOutOfRangeException()
            };
        }

        public string GetName(int i) => _nameToIndex.First(kv => kv.Value == i).Key;
        public int GetOrdinal(string name) => _nameToIndex.TryGetValue(name, out var idx) ? idx : -1;

        public bool IsDBNull(int i)
        {
            var value = GetValue(i);
            return value == null || value == DBNull.Value;
        }

        public int GetValues(object[] values)
        {
            var count = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < count; i++)
                values[i] = GetValue(i);
            return count;
        }

        // Mandatory interface members
        public bool NextResult() => false;
        public int RecordsAffected => -1;
        public bool IsClosed => false;
        public int Depth => 0;
        public void Close() => Dispose();
        public void Dispose() => _enumerator.Dispose();

        // Simplified typed getters
        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(GetOrdinal(name));
        public string GetDataTypeName(int i) => GetFieldType(i).Name;
        public Type GetFieldType(int i) => GetValue(i).GetType();

        // Unused members
        public bool GetBoolean(int i) => (bool)GetValue(i);
        public byte GetByte(int i) => (byte)GetValue(i);
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public char GetChar(int i) => (char)GetValue(i);
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
        public decimal GetDecimal(int i) => (decimal)GetValue(i);
        public double GetDouble(int i) => Convert.ToDouble(GetValue(i));
        public float GetFloat(int i) => Convert.ToSingle(GetValue(i));
        public Guid GetGuid(int i) => (Guid)GetValue(i);
        public short GetInt16(int i) => Convert.ToInt16(GetValue(i));
        public int GetInt32(int i) => Convert.ToInt32(GetValue(i));
        public long GetInt64(int i) => Convert.ToInt64(GetValue(i));
        public string GetString(int i) => GetValue(i)?.ToString() ?? string.Empty;
        public DataTable GetSchemaTable() => throw new NotSupportedException();
    }
}
