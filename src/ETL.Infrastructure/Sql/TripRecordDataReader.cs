using System.Data;
using ETL.Core.Models;

namespace ETL.Infrastructure.Sql
{
    public class TripRecordDataReader : IDataReader
    {
        private readonly IEnumerator<TripRecord> _enumerator;
        private bool _hasRow;

        public TripRecordDataReader(IAsyncEnumerable<TripRecord> rows)
        {
            // materialize asynchronously partially isn't trivial synchronously;
            // for simplicity read to a queue
            var list = new List<TripRecord>();
            var t = Task.Run(async () =>
            {
                await foreach (var r in rows)
                {
                    list.Add(r);
                }
            });
            t.GetAwaiter().GetResult();
            _enumerator = list.GetEnumerator();
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

        private TripRecord Current { get; set; } = default!;

        public bool Read()
        {
            if (_enumerator.MoveNext())
            {
                Current = _enumerator.Current;
                return true;
            }
            return false;
        }

        public int FieldCount => 9;
        public void Dispose() => _enumerator.Dispose();
        public bool NextResult() => false;
        public int RecordsAffected => -1;
        public bool IsClosed => false;
        public int Depth => 0;
        public string GetName(int i) => i switch
        {
            0 => "tpep_pickup_datetime",
            1 => "tpep_dropoff_datetime",
            2 => "passenger_count",
            3 => "trip_distance",
            4 => "store_and_fwd_flag",
            5 => "PULocationID",
            6 => "DOLocationID",
            7 => "fare_amount",
            8 => "tip_amount",
            _ => throw new IndexOutOfRangeException()
        };
        public Type GetFieldType(int i) => i switch
        {
            0 => typeof(DateTime),
            1 => typeof(DateTime),
            2 => typeof(short),
            3 => typeof(decimal),
            4 => typeof(string),
            5 => typeof(int),
            6 => typeof(int),
            7 => typeof(decimal),
            8 => typeof(decimal),
            _ => throw new IndexOutOfRangeException()
        };

        #region NotImplementedMembers
        public object this[int i] => GetValue(i);
        public object this[string name] => throw new NotImplementedException();
        public bool GetBoolean(int i) => (bool)GetValue(i);
        public byte GetByte(int i) => (byte)GetValue(i);
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotImplementedException();
        public char GetChar(int i) => (char)GetValue(i);
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotImplementedException();
        public IDataReader GetData(int i) => throw new NotImplementedException();
        public string GetDataTypeName(int i) => GetFieldType(i).Name;
        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
        public decimal GetDecimal(int i) => (decimal)GetValue(i);
        public double GetDouble(int i) => (double)GetValue(i);
        public Type GetProviderSpecificFieldType(int i) => GetFieldType(i);
        public float GetFloat(int i) => (float)GetValue(i);
        public Guid GetGuid(int i) => (Guid)GetValue(i);
        public short GetInt16(int i) => (short)GetValue(i);
        public int GetInt32(int i) => (int)GetValue(i);
        public long GetInt64(int i) => (long)GetValue(i);
        public string GetString(int i) => (string)GetValue(i);
        public int GetOrdinal(string name) => throw new NotImplementedException();
        public DataTable GetSchemaTable() => throw new NotImplementedException();
        public int GetValues(object[] values) => throw new NotImplementedException();
        public bool IsDBNull(int i) => throw new NotImplementedException();
        public void Close() { }
        #endregion
    }
}
